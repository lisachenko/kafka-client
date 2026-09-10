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
 * Creates and deletes topics through the protocol, the way Kafka 0.10.1 made possible.
 *
 * Before CreateTopics (api key 19) and DeleteTopics (key 20) a client could only ask for the metadata of a topic
 * that did not exist and hope that the broker ran with `auto.create.topics.enable=true`, which creates a topic with
 * the default partition count and no options and reports nothing about what happened. The two apis are answered by
 * the **controller** alone; `AdminClient` looks it up in the `controller_id` of a Metadata v1/v2 answer and repeats
 * a request once against a freshly looked up controller when the answer says 41 (NotController).
 *
 * Neither method throws for a topic: the result holds one entry per requested topic, in the order of the request,
 * `null` when it worked and the exception of its error code when it did not - one topic of a batch says nothing
 * about the others.
 *
 *   docker compose up -d
 *   php examples/create-topic.php
 *   php examples/create-topic.php my-topic                 # another topic name
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/create-topic.php
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewPartitions;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;

require dirname(__DIR__) . '/vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$topic            = $argv[1] ?? 'kafka-client-example-created';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . trim(explode(',', $bootstrapServers)[0])],
    ClientConfig::CLIENT_ID         => 'kafka-client-example-admin',
];

$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

/**
 * Prints what `createTopics()`/`deleteTopics()` answered for every topic of a request
 *
 * @param array<string, ?Protocol\Kafka\Common\Errors\KafkaException> $results
 */
function report(string $what, array $results): void
{
    echo $what, "\n";
    foreach ($results as $topicName => $error) {
        if ($error === null) {
            echo "  {$topicName}: ok\n";

            continue;
        }
        // The context of the exception carries the error_message that version 1 of CreateTopics added
        echo "  {$topicName}: error {$error->getCode()} - {$error->getMessage()}\n";
    }
}

echo "The controller of the cluster is node " . $admin->findController()->nodeId . "\n\n";

// validate_only (version 1 of the api, Kafka 0.10.2) runs every check the controller would run and creates nothing
report(
    "Validating {$topic} without creating it",
    $admin->createTopics([new NewTopic($topic, 3, 1)], 30000, validateOnly: true)
);
echo '  in listTopics(): ' . (in_array($topic, $admin->listTopics(), true) ? 'yes' : "no, as expected\n");

// Three partitions with one replica each, placed by the controller, plus a topic-level option
report(
    "\nCreating {$topic}",
    $admin->createTopics([new NewTopic($topic, 3, 1, configs: ['retention.ms' => '3600000'])])
);

// The same request again: the controller answers 36 (TopicAlreadyExists) with the message of its own exception
report("\nCreating {$topic} a second time", $admin->createTopics([new NewTopic($topic, 3, 1)]));

// An explicit assignment names the brokers of every partition itself, the preferred leader first. Partitions and a
// replication factor may not be given at the same time - the broker answers 42 (InvalidRequest) for that.
$assigned = $topic . '-assigned';
report(
    "\nCreating {$assigned} with an explicit replica assignment",
    $admin->createTopics([NewTopic::withReplicaAssignment($assigned, [0 => [0], 1 => [0]])])
);

$cluster->reload();
echo "\nPartitions the controller created\n";
foreach ($admin->describeTopics([$topic, $assigned]) as $name => $metadata) {
    echo "  {$name}: " . count($metadata->partitions) . " partition(s)\n";
}

// CreatePartitions (key 37, Kafka 1.0, KIP-195) raises the partition count of a topic that exists. The number is
// what the topic should have AFTERWARDS, and the api can only ever grow a topic: a count that is not above the
// current one is answered with 37 (InvalidPartitions).
report("\nRaising {$topic} to five partitions", $admin->createPartitions([$topic => 5]));
report("\nAsking for four partitions again", $admin->createPartitions([$topic => 4]));

// NewPartitions::increaseTo() also names the brokers of every ADDED partition, the preferred leader first
report(
    "\nAdding a partition on a named broker",
    $admin->createPartitions([$topic => NewPartitions::increaseTo(6, [[0]])])
);

$cluster->reload();
echo '  ' . $topic . ' now has ' . count($admin->describeTopics([$topic])[$topic]->partitions) . " partition(s)\n";

// A deletion needs `delete.topic.enable=true` on the broker; with a timeout of 0 the answer is 7 (RequestTimedOut)
// although the deletion is under way and finishes a moment later.
report("\nDeleting both topics", $admin->deleteTopics([$topic, $assigned]));

// A topic the cluster does not have is answered with 3 (UnknownTopicOrPartition), and nothing is created for it
report("\nDeleting a topic that does not exist", $admin->deleteTopics(['kafka-client-example-no-such-topic']));
