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
 * The three admin apis Kafka 0.11 added: DescribeConfigs (32), AlterConfigs (33) and DeleteRecords (21).
 *
 * Before them a client had to talk to **ZooKeeper** to read or change the configuration of a topic, and there was
 * no way at all to remove records from a partition before their retention was over. KIP-133 moved the
 * configuration into the protocol and KIP-107 added the api that moves the **low watermark** of a partition
 * forward, which is what a data-retention request of an application boils down to.
 *
 * Three properties of these apis are worth knowing, and the example shows all three:
 *
 *  - `alterConfigs()` **replaces** the whole configuration of a topic: an option that was set and is not in the
 *    request is reset to its default, which is what `Config::nonDefaultValues()` exists for;
 *  - a 0.11 broker alters **topics only** - a broker resource is refused with 42, dynamic broker configuration is
 *    Kafka 1.1 - and `describeConfigs()` of a broker resource is answered by that broker alone;
 *  - `deleteRecords()` is served by the **leader** of every partition, like a produce, and answers the new low
 *    watermark; deleting at or below the current one is not an error.
 *
 *   docker compose up -d
 *   php examples/admin-configs.php
 *   php examples/admin-configs.php my-topic
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/admin-configs.php
 *
 * @see docs/protocol/0.11.0.md, sections "DescribeConfigs API (key 32, v0)", "AlterConfigs API (key 33, v0)" and
 *      "DeleteRecords API (key 21, v0)"
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

require dirname(__DIR__) . '/vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$topic            = $argv[1] ?? 'kafka-client-example-configs';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . trim(explode(',', $bootstrapServers)[0])],
    ClientConfig::CLIENT_ID         => 'kafka-client-example-configs',
];

$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

// One partition, so that the offsets of the DeleteRecords part below are predictable
$created = $admin->createTopics([new NewTopic($topic, 1, 1, configs: ['retention.ms' => '7200000'])]);
if ($created[$topic] !== null && $created[$topic]->getCode() !== 36) {  // 36 = the topic already exists
    echo 'Could not create the topic: ' . $created[$topic]->getMessage() . "\n";

    exit(1);
}
$cluster->reload();

$topicResource = ConfigResource::topic($topic);
$topicKey      = $topicResource->key();                    // "topic:kafka-client-example-configs"

// A resource is named with a ConfigResource and addressed in the result by its key(), because PHP cannot use an
// object as an array key. Without a list of option names the broker answers all ~40 entries of a topic.
$configs = $admin->describeConfigs([$topicResource], ['retention.ms', 'cleanup.policy', 'message.format.version']);

echo "Configuration of {$topic}\n";
foreach ($configs[$topicKey]->entries as $entry) {
    printf(
        "  %-24s = %-12s %s%s",
        $entry->name,
        $entry->value ?? '<null>',
        $entry->isDefault ? '(default)' : '(set on the topic)',
        PHP_EOL
    );
}

// nonDefaultValues() is the whole point of the "replaces everything" rule: read what the topic really has, change
// one option, and send the result back. Without it `retention.ms` alone would reset every other option.
$current = $admin->describeConfigs([$topicResource])[$topicKey]->nonDefaultValues();
$altered = $admin->alterConfigs([$topicKey => ['retention.ms' => '3600000'] + $current]);

echo "\nalterConfigs(): " . ($altered[$topicKey] === null
    ? 'ok, retention.ms is now ' . $admin->describeConfigs([$topicResource], ['retention.ms'])[$topicKey]->value('retention.ms')
    : 'error ' . $altered[$topicKey]->getCode() . ' - ' . $altered[$topicKey]->getMessage()) . "\n";

// Nothing is thrown for a resource that was refused: the result has one entry per resource, exactly like
// createTopics(). A broker resource is answered 42, because a 0.11 broker cannot change its own configuration.
$brokerKey  = ConfigResource::broker(0)->key();
$refused    = $admin->alterConfigs([$brokerKey => ['log.retention.hours' => '1']]);
echo 'A broker resource is refused with the code ' . ($refused[$brokerKey]?->getCode() ?? 0) . ': '
    . ($refused[$brokerKey]?->getContext()['error'] ?? 'accepted?!') . "\n";

// The live KafkaConfig of a broker, which only that broker can answer; every entry of it is read-only, and the
// value of a sensitive option (a password) is never sent and arrives as null
$brokerConfig = $admin->describeConfigs([ConfigResource::broker(0)], ['log.retention.hours', 'num.partitions']);
echo "\nConfiguration of broker 0\n";
foreach ($brokerConfig[$brokerKey]->entries as $entry) {
    printf("  %-24s = %s%s", $entry->name, $entry->value ?? '<null>', PHP_EOL);
}

$producer = new KafkaProducer($configuration + [ProducerConfig::ACKS => ProducerConfig::ACKS_ALL]);
for ($index = 0; $index < 5; $index++) {
    $producer->send($topic, Record::fromValue("record #{$index}"), 0);
}
$producer->flush();

$earliest = $admin->listOffsets([$topic => [0]], OffsetsRequest::EARLIEST)[$topic][0];
$latest   = $admin->listOffsets([$topic => [0]], OffsetsRequest::LATEST)[$topic][0];
echo "\n{$topic}:0 holds the offsets {$earliest} to " . ($latest - 1) . "\n";

// Everything below the offset becomes unreadable at once; the record AT the offset stays. The answer is the new
// low watermark of every partition, which is the same number that Offsets(EARLIEST) reports afterwards.
$deleted = $admin->deleteRecords([$topic => [0 => $earliest + 2]]);
echo 'After deleteRecords(before ' . ($earliest + 2) . '): low watermark ' . $deleted[$topic][0]->lowWatermark . "\n";

// RecordsToDelete::allRecords() is the -1 of the wire: everything up to the high watermark of the partition
$purged = $admin->deleteRecords([$topic => [0 => RecordsToDelete::allRecords()]]);
echo 'After deleteRecords(allRecords): low watermark ' . $purged[$topic][0]->lowWatermark
    . ", the partition is empty but its offsets keep counting\n";

// A consumer that was reading below the new watermark gets 1 (OffsetOutOfRange) on its next fetch, and a fetch
// answer reports the same number in the `log_start_offset` that Fetch v5 added.
echo 'Offsets(EARLIEST) agrees: ' . $admin->listOffsets([$topic => [0]], OffsetsRequest::EARLIEST)[$topic][0] . "\n";

$admin->deleteTopics([$topic]);
echo "\nDeleted {$topic} again.\n";
