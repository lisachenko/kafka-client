<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * Admin API example for the Kafka 1.1.1 protocol: the cluster and its controller, the api table of a broker, the
 * topics, the offsets of a partition, and the consumer groups - listed, described and, since Kafka 1.1 (KIP-229),
 * deleted through the protocol with `deleteConsumerGroups()`.
 *
 * The configuration apis of the same client are in {@see examples/admin-configs.php}, the log directories of a
 * broker in {@see examples/admin-log-dirs.php}, the topic apis in {@see examples/create-topic.php} and the
 * delegation tokens of KIP-48 in {@see examples/delegation-tokens.php}.
 *
 * Start the broker of docker-compose.yml and run:
 *
 *   docker compose up -d
 *   php examples/admin.php [topic] [groupId]
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v6)", "ListGroups API (key 16, v0 to v2)",
 *      "DescribeGroups API (key 15, v0 to v3)" and "DeleteGroups API (key 42, v0 and v1)"
 */

declare(strict_types=1);

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

require __DIR__ . '/../vendor/autoload.php';

$topic   = $argv[1] ?? 'example-topic';
$groupId = $argv[2] ?? 'example-group';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . (getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092')],
    ClientConfig::CLIENT_ID         => 'admin-example',
    // Where the group offsets live: `kafka` uses OffsetFetch v2, `zookeeper` the ZooKeeper-backed v0
    ClientConfig::OFFSETS_STORAGE   => ClientConfig::OFFSETS_STORAGE_KAFKA,
];

$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

// Metadata v1 and v2 (Kafka 0.10.0 / 0.10.1) added the identity of the cluster and of its controller, and a rack
// for every broker; a cluster that is still electing a controller answers null for it.
echo "Cluster {$cluster->clusterId()}\n";
echo 'Controller: ' . ($cluster->controller()?->nodeId ?? 'none yet') . "\n";

echo "\nBrokers\n";
foreach ($admin->findAllBrokers() as $broker) {
    $rack = $broker->rack === null ? 'no rack' : "rack {$broker->rack}";
    echo "  {$broker->nodeId}: {$broker->host}:{$broker->port} ({$rack})\n";
}

// ApiVersions (key 18) is what Kafka 0.10.0 added so that a client can ask what the broker speaks
echo "\nApis of the first broker\n";
$brokers = $admin->findAllBrokers();
$apis    = $admin->getApiVersions(reset($brokers));
echo '  ' . count($apis) . " api keys, Produce up to v{$apis[0]->maxVersion}, Fetch up to v{$apis[1]->maxVersion}\n";

echo "\nTopics\n";
foreach ($admin->listTopics() as $name) {
    echo "  {$name}\n";
}

// Describing a topic is a question and no longer a side effect: Metadata v4 (Kafka 0.11, KIP-4) added
// `allow_auto_topic_creation`, and every request of the admin client sends it as false, so a topic that does not
// exist is answered with the error code 3 and stays absent - where every version below 4 would have created it on
// a broker with auto.create.topics.enable=true. CreateTopics (Kafka 0.10.1) is the explicit way, and it reports
// what went wrong - see examples/create-topic.php.
echo "\nPartitions of {$topic}\n";
$created = $admin->createTopics([new NewTopic($topic, 3, 1)])[$topic] ?? null;
if ($created !== null && $created->getCode() !== KafkaException::TOPIC_ALREADY_EXISTS) {
    echo '  the topic could not be created: ' . $created->getMessage() . "\n";

    return;
}

$cluster->reload();
$metadata = $admin->describeTopics([$topic])[$topic] ?? null;
if ($metadata === null || $metadata->partitions === []) {
    echo "  the controller has not elected the leaders yet, run this example again in a moment\n";

    return;
}
foreach ($metadata->partitions as $partition) {
    $replicas = implode(',', $partition->replicas);
    echo "  {$partition->partitionId}: leader {$partition->leader}, replicas [{$replicas}]\n";
}

// The Offsets api is served by the leader of each partition, so the cluster metadata has to be fresh
$cluster->reload();
$partitions = array_keys($metadata->partitions);

// Version 1 of the Offsets api (Kafka 0.10.1) answers ONE offset per partition, not a list of segment offsets
echo "\nOffsets of {$topic}\n";
$earliest = $admin->listOffsets([$topic => $partitions], OffsetsRequest::EARLIEST);
$latest   = $admin->listOffsets([$topic => $partitions]);
foreach ($partitions as $partition) {
    $first = $earliest[$topic][$partition] ?? 0;
    $last  = $latest[$topic][$partition] ?? 0;
    echo "  {$partition}: {$first} .. {$last} (" . ($last - $first) . " messages)\n";
}

// OffsetFetch v2 (Kafka 0.10.2) made the topic array nullable: without a partition list the coordinator answers
// every topic-partition this group has ever committed, which is what listGroupOffsets() asks for by default.
echo "\nCommitted offsets of the group {$groupId}\n";
echo "  coordinator: node " . $admin->findCoordinator($groupId)->nodeId . "\n";
foreach ($admin->listGroupOffsets($groupId) as $topicOffsets) {
    foreach ($topicOffsets->partitions as $partitionId => $partition) {
        $committed = $partition->offset === -1 ? 'nothing committed yet' : (string) $partition->offset;
        echo "  {$topicOffsets->topic}-{$partitionId}: {$committed}\n";
    }
}

// Kafka 0.9 moved the consumer groups out of ZooKeeper into the brokers: each of them coordinates a share of the
// groups and reports only its own, so the list of the cluster is the union of all of their answers.
echo "\nConsumer groups of the cluster\n";
$groups = $admin->listAllGroups();
if ($groups === []) {
    echo "  not a single group has a member at the moment\n";
}
foreach ($groups as $listedGroupId => $listedGroup) {
    echo "  {$listedGroupId} ({$listedGroup->protocolType})\n";
}

// DescribeGroups is answered by the coordinator of the group. A group that has no members - because nobody has
// joined it, or because everybody has left - is reported with the state Dead and the error code 0, not as an error.
echo "\nDescription of the group {$groupId}\n";
$description = $admin->describeGroup($groupId);
echo "  state: {$description->state}\n";
echo "  protocol type: '{$description->protocolType}', protocol: '{$description->protocol}'\n";
foreach ($description->members as $memberId => $member) {
    $assignmentSize = strlen($member->memberAssignment);
    echo "  member {$memberId} of the client {$member->clientId} at {$member->clientHost}, "
        . "{$assignmentSize} bytes of assignment\n";
}

// DeleteGroups (key 42, Kafka 1.1, KIP-229) makes the coordinator forget a group and the offsets it committed.
// Only a group without members can be deleted: one that still has a live consumer is answered with 68
// (NonEmptyGroup), and a group the coordinator has never heard of with 69 (GroupIdNotFound). Nothing is thrown -
// the result has one entry per group, like createTopics().
echo "\nDeleting the group {$groupId}\n";
foreach ($admin->deleteConsumerGroups([$groupId]) as $deletedGroupId => $error) {
    echo '  ' . $deletedGroupId . ': ' . ($error === null
        ? 'deleted, with every offset it had committed'
        : 'error ' . $error->getCode() . ' - ' . $error->getMessage()) . "\n";
}

// The remaining admin call, controlledShutdown(), asks the controller to move every leader off a broker. It is what
// kafka-server-stop.sh triggers, and it really does stop serving that broker - only send it to a broker you want to
// shut down. Creating and deleting topics is in examples/create-topic.php, the offsets by timestamp of Kafka 0.10.1
// in examples/offsets-for-times.php.
