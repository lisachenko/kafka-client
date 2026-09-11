<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

/**
 * The two JBOD apis Kafka 1.0 added: DescribeLogDirs (35) and AlterReplicaLogDirs (34), KIP-113.
 *
 * A broker may be given more than one data directory - `log.dirs` is a list, and the container of
 * `docker-compose.yml` runs with `/tmp/kafka-logs` and `/tmp/kafka-logs-2` - and every replica lives in exactly
 * one of them. Before KIP-113 that was invisible from outside; these two apis say which disk a replica is on and
 * ask for it to be moved to another one.
 *
 * Three properties are worth knowing, and the example shows all three:
 *
 *  - both apis are **broker-local**: a broker only knows its own disks, so `describeLogDirs()` takes a list of
 *    broker ids and `alterReplicaLogDirs()` is keyed by a {@see \Protocol\Kafka\Admin\TopicPartitionReplica},
 *    which names one;
 *  - the request array of `describeLogDirs()` is **nullable**: `null` asks for every replica of the broker - a
 *    large answer on a busy cluster - while an empty array asks only for the directories themselves;
 *  - `alterReplicaLogDirs()` answers as soon as the move is **accepted**. The copy runs in the background, and
 *    while it does the replica is reported in *both* directories: the destination carries `isFuture = true` and
 *    an `offsetLag` that counts down. `describeLogDirs()` is what says when it is done.
 *
 *   docker compose up -d
 *   php examples/admin-log-dirs.php
 *   php examples/admin-log-dirs.php my-topic
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/admin-log-dirs.php
 *
 * @see docs/protocol/2.8.md, sections "DescribeLogDirs API (key 35, v0 and v1)" and
 *      "AlterReplicaLogDirs API (key 34, v0 and v1)"
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\TopicPartitionReplica;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;

require_once __DIR__ . '/../vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$topic            = $argv[1] ?? 'example-log-dirs';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . $bootstrapServers],
    ClientConfig::CLIENT_ID         => 'example-log-dirs',
    ProducerConfig::ACKS            => 1,
];

$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

$brokerIds = array_keys($cluster->nodes());
echo 'Brokers of the cluster: ', implode(', ', $brokerIds), "\n\n";

// 1. Which disks does each broker have? An EMPTY selection asks for the directories and no replica at all,
//    which is the cheapest form of the request - a `null` one would report every partition of the broker.
echo "The log directories of every broker\n";
foreach ($admin->describeLogDirs($brokerIds, []) as $brokerId => $directories) {
    foreach ($directories as $path => $directory) {
        $state = $directory->error === null ? 'online' : 'ERROR ' . $directory->error->getMessage();
        echo "  broker {$brokerId}: {$path} ({$state})\n";
    }
}
echo "\n";

// 2. Create a topic and write a few records into it, so that there is a replica to look at and to move
$created = $admin->createTopics([new NewTopic($topic, 1, 1)]);
if ($created[$topic] !== null) {
    echo "The topic {$topic} is already there: ", $created[$topic]->getMessage(), "\n";
}

$producer = new KafkaProducer($configuration);
for ($index = 0; $index < 200; $index++) {
    $producer->send($topic, Record::fromValue(str_repeat("record-{$index} ", 64)), 0);
}
$producer->flush();
$cluster->reload();

$brokerId = $brokerIds[0];
$replica  = TopicPartitionReplica::of($topic, 0, $brokerId);

// 3. Where does the partition live, and how big is it?
$directories = $admin->describeLogDirs([$brokerId], [$topic => [0]])[$brokerId];
$source      = null;
foreach ($directories as $path => $directory) {
    $info = $directory->replica($topic, 0);
    if ($info !== null) {
        $source = $path;
        echo "{$replica} lives in {$path}: {$info->size} bytes, offset lag {$info->offsetLag}\n\n";
    }
}

if ($source === null) {
    echo "The broker {$brokerId} does not host {$replica}, nothing to move\n";

    exit(0);
}

// 4. Move it into another directory of the same broker. The answer only says that the move was accepted.
$target = null;
foreach (array_keys($directories) as $path) {
    if ($path !== $source) {
        $target = $path;

        break;
    }
}

if ($target === null) {
    echo "The broker has a single log directory, so there is nowhere to move {$replica} to\n";

    exit(0);
}

$answer = $admin->alterReplicaLogDirs([$replica->key() => $target]);
if ($answer[$replica->key()] !== null) {
    echo 'The broker refused the move: ', $answer[$replica->key()]->getMessage(), "\n";

    exit(1);
}
echo "The move of {$replica} to {$target} was accepted\n";

// 5. Watch it: the replica is in both directories until the mover swaps the logs in
$deadline = microtime(true) + 60.0;
do {
    $directories = $admin->describeLogDirs([$brokerId], [$topic => [0]])[$brokerId];
    $future      = $directories[$target]->replica($topic, 0);
    $current     = $directories[$source]->replica($topic, 0);

    if ($future !== null && $future->isFuture) {
        echo "  copying: {$future->size} bytes in {$target}, {$future->offsetLag} records still to go\n";
    }
    if ($current === null && $future !== null && !$future->isFuture) {
        echo "  done: {$replica} is now served from {$target} ({$future->size} bytes)\n";

        break;
    }
    usleep(50000);
} while (microtime(true) < $deadline);

// 6. And the two error codes the api has of its own
$refused = $admin->alterReplicaLogDirs([$replica->key() => '/there/is/no/such/log/dir']);
echo "\nA path that is not in log.dirs: ", $refused[$replica->key()]?->getMessage() ?? 'accepted', "\n";

$absent = TopicPartitionReplica::of($topic . '-does-not-exist', 0, $brokerId);
$missed = $admin->alterReplicaLogDirs([$absent->key() => $target]);
echo 'A replica the broker does not host: ', $missed[$absent->key()]?->getMessage() ?? 'accepted', "\n";

echo "\nEvery replica of the broker, the way `kafka-log-dirs.sh --describe` asks for it:\n";
$everything = 0;
foreach ($admin->describeLogDirs([$brokerId]) as $directories) {
    foreach ($directories as $path => $directory) {
        $everything += count($directory->replicaInfos);
        echo '  ', $path, ': ', count($directory->replicaInfos), " replicas\n";
    }
}
echo "  {$everything} replicas in total - which is why a client that knows its partitions names them\n";

$admin->deleteTopics([$topic]);
echo "\nThe topic {$topic} was deleted again\n";
