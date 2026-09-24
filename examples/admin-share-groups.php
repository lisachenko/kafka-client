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
 * The admin side of the share groups of KIP-932 (Kafka 4.1): list them, read where they stand, move them, forget
 * topics and delete them.
 *
 * A share group commits no offset. Its members acknowledge records one by one, and the share coordinator keeps
 * one **start offset** per partition - the earliest record the group is not done with - and, since Kafka 4.2
 * (KIP-1226), the **lag**: the records from there to the end of the partition the group still has to deliver.
 *
 *  - `listShareGroups()` is `listGroups(ListGroupsOptions.forShareGroups())` of the Java admin client;
 *  - `listShareGroupOffsets()` reads the start offset, the leader epoch and the lag of every partition
 *    (DescribeShareGroupOffsets v1), `null` for a partition the group holds no state of;
 *  - `alterShareGroupOffsets()` sets start offsets - and creates the share group when it does not exist yet -
 *    while the group has no member (AlterShareGroupOffsets);
 *  - `deleteShareGroupOffsets()` makes the group forget whole topics (DeleteShareGroupOffsets);
 *  - `deleteShareGroups()` deletes empty share groups with DeleteGroups, the api of every group type.
 *
 *   docker compose up -d
 *   php examples/admin-share-groups.php
 *   php examples/admin-share-groups.php my-topic my-share-group
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/admin-share-groups.php
 *
 * @see docs/protocol/4.3.md, section "The share-group admin methods"
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\SharePartitionOffsetInfo;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;

require_once __DIR__ . '/../vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$topic            = $argv[1] ?? 'example-share-groups';
$group            = $argv[2] ?? 'example-share-group';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . $bootstrapServers],
    ClientConfig::CLIENT_ID         => 'example-share-groups',
    ProducerConfig::ACKS            => 1,
];

$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

// 1. A topic with ten records in its one partition
$created = $admin->createTopics([new NewTopic($topic, 1, 1)]);
echo "Topic {$topic}: ", $created[$topic] === null ? 'created' : 'already there', "\n";
$producer = new KafkaProducer($configuration);
for ($index = 0; $index < 10; $index++) {
    $producer->send($topic, Record::fromValue("record-{$index}"), 0);
}
$producer->flush();
echo "Ten records written\n\n";

$describe = static function (AdminClient $admin, string $group): void {
    $offsets = $admin->listShareGroupOffsets([$group => null])[$group];
    if ($offsets === []) {
        echo "  no state of any partition\n";
    }
    foreach ($offsets as $topic => $partitions) {
        foreach ($partitions as $partition => $info) {
            echo $info instanceof SharePartitionOffsetInfo
                ? "  {$topic}-{$partition}: start offset {$info->startOffset}, lag " . ($info->lag ?? 'unknown') . "\n"
                : "  {$topic}-{$partition}: no start offset\n";
        }
    }
};

// 2. Setting a start offset creates the share group, which has no member yet. Every partition gets an entry: null
//    when its offset was set, the exception of its error code otherwise - 68 NonEmptyGroup for a group with a
//    member, 69 GroupIdNotFound for a classic or KIP-848 group of the same name.
foreach ($admin->alterShareGroupOffsets($group, [$topic => [0 => 4]]) as $partitions) {
    foreach ($partitions as $partition => $error) {
        echo "Start offset of {$topic}-{$partition}: ", $error === null ? 'set to 4' : $error->getMessage(), "\n";
    }
}

// 3. The group is a share group of the cluster now, Empty until a member joins
foreach ($admin->listShareGroups() as $groupId => $listing) {
    echo "Share group {$groupId}: {$listing->groupState}\n";
}

// 4. Where it stands: the start offset 4 and the lag 6 - the six records it still has to deliver. The node may take
//    a moment to compute the lag after an alter.
echo "\nThe offsets of {$group} (what `kafka-share-groups.sh --describe --offsets` prints)\n";
$describe($admin, $group);

// 5. Forget the topic: a member that reads it later starts over at the group config `share.auto.offset.reset`
$deleted = $admin->deleteShareGroupOffsets($group, [$topic]);
echo "\nState of {$topic}: ", $deleted[$topic] === null ? 'deleted' : $deleted[$topic]->getMessage(), "\n";
$describe($admin, $group);

// 6. Delete the group - it has no member - and the topic. DeleteGroups deletes a group of any type, so name share
//    groups alone here: listShareGroups() says which ones they are.
$error = $admin->deleteShareGroups([$group])[$group];
echo "\nShare group {$group}: ", $error === null ? 'deleted' : $error->getMessage(), "\n";
$admin->deleteTopics([$topic]);
echo "The topic {$topic} was deleted again\n";
