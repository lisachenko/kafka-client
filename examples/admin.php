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
 * Admin API example for the Kafka 0.9.0.1 protocol.
 *
 * Start the broker of docker-compose.yml and run:
 *
 *   docker compose up -d
 *   php examples/admin.php [topic] [groupId]
 */

declare(strict_types=1);

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

require __DIR__ . '/../vendor/autoload.php';

$topic   = $argv[1] ?? 'example-topic';
$groupId = $argv[2] ?? 'example-group';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . (getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092')],
    ClientConfig::CLIENT_ID         => 'admin-example',
    // Where the group offsets live: `kafka` uses OffsetFetch v1, `zookeeper` the ZooKeeper-backed v0
    ClientConfig::OFFSETS_STORAGE   => ClientConfig::OFFSETS_STORAGE_KAFKA,
];

$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

echo "Brokers\n";
foreach ($admin->findAllBrokers() as $broker) {
    echo "  {$broker->nodeId}: {$broker->host}:{$broker->port}\n";
}

echo "\nTopics\n";
foreach ($admin->listTopics() as $name) {
    echo "  {$name}\n";
}

// CAVEAT: a topic that does not exist yet is CREATED by this call when the broker runs with
// auto.create.topics.enable=true. Kafka 0.9 has no CreateTopics api - that arrived in 0.10.1 - so a Metadata
// request is the only way a client can create a topic at all. The first answer reports the topic error code 5
// (LeaderNotAvailable) and no partitions until the controller has elected the partition leaders.
echo "\nPartitions of {$topic}\n";
$metadata = $admin->describeTopics([$topic])[$topic] ?? null;
if ($metadata === null || $metadata->partitions === []) {
    echo "  the topic is being created, run this example again in a moment\n";

    return;
}
foreach ($metadata->partitions as $partition) {
    $replicas = implode(',', $partition->replicas);
    echo "  {$partition->partitionId}: leader {$partition->leader}, replicas [{$replicas}]\n";
}

// The Offsets api is served by the leader of each partition, so the cluster metadata has to be fresh
$cluster->reload();
$partitions = array_keys($metadata->partitions);

echo "\nOffsets of {$topic}\n";
$earliest = $admin->listOffsets([$topic => $partitions], OffsetsRequest::EARLIEST);
$latest   = $admin->listOffsets([$topic => $partitions]);
foreach ($partitions as $partition) {
    $first = $earliest[$topic][$partition] ?? 0;
    $last  = $latest[$topic][$partition] ?? 0;
    echo "  {$partition}: {$first} .. {$last} (" . ($last - $first) . " messages)\n";
}

// 0.9 has no "every topic of this group" request - the nullable topic array of OffsetFetch is version 2 of that
// api and arrived with Kafka 0.10.2 - so the partitions whose committed offsets are wanted have to be named
// explicitly.
echo "\nCommitted offsets of the group {$groupId}\n";
echo "  coordinator: node " . $admin->findCoordinator($groupId)->nodeId . "\n";
foreach ($admin->listGroupOffsets($groupId, [$topic => $partitions]) as $topicOffsets) {
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

// The remaining admin call, controlledShutdown(), asks the controller to move every leader off a broker. It is what
// kafka-server-stop.sh triggers, and it really does stop serving that broker - only send it to a broker you want to
// shut down. There is no ApiVersions api to call here: that is key 18 and arrived with Kafka 0.10.
