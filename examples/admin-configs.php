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
 * The configuration apis of a Kafka 1.1 broker: DescribeConfigs (32), AlterConfigs (33) and DeleteRecords (21).
 *
 * Before them a client had to talk to **ZooKeeper** to read or change the configuration of a topic, and there was
 * no way at all to remove records from a partition before their retention was over. KIP-133 (Kafka 0.11) moved the
 * configuration into the protocol, KIP-107 added the api that moves the **low watermark** of a partition forward,
 * and KIP-226 (Kafka 1.1) added the **version 1** of DescribeConfigs and the dynamic broker configuration.
 *
 * Four properties of these apis are worth knowing, and the example shows all four:
 *
 *  - every entry of a DescribeConfigs v1 answer says WHERE its value comes from (`ConfigEntry::$source`) and, with
 *    `$includeSynonyms`, every other place the broker looked (`ConfigEntry::$synonyms`);
 *  - `alterConfigs()` **replaces** the whole configuration of a resource: an option that was set and is not in the
 *    request is reset, which is what `Config::ownValues()` exists for - `nonDefaultValues()` also reports what the
 *    broker configuration gives the topic, and sending that back would write it into the topic;
 *  - a 1.1 broker alters a **broker** resource as well, but only the options of `AllDynamicConfigs`; a static one
 *    is refused with 42 and the names it cannot update. `describeConfigs()` of a broker resource is answered by
 *    that broker alone, and the resource with an EMPTY name is the cluster-wide default;
 *  - `deleteRecords()` is served by the **leader** of every partition, like a produce, and answers the new low
 *    watermark; deleting at or below the current one is not an error.
 *
 *   docker compose up -d
 *   php examples/admin-configs.php
 *   php examples/admin-configs.php my-topic
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/admin-configs.php
 *
 * @see docs/protocol/2.8.md, sections "DescribeConfigs API (key 32, v0, v1 and v2)", "AlterConfigs API (key 33, v0 and v1)" and
 *      "DeleteRecords API (key 21, v0 and v1)"
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\ConfigSource;
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
// object as an array key. Without a list of option names the broker answers all ~40 entries of a topic; the third
// argument is the `include_synonyms` of KIP-226, which fills ConfigEntry::$synonyms.
$configs = $admin->describeConfigs(
    [$topicResource],
    ['retention.ms', 'cleanup.policy', 'segment.bytes'],
    true
);

echo "Configuration of {$topic}\n";
foreach ($configs[$topicKey]->entries as $entry) {
    printf(
        "  %-24s = %-12s %s%s",
        $entry->name,
        $entry->value ?? '<null>',
        ConfigSource::nameOf($entry->source),
        PHP_EOL
    );
    foreach ($entry->synonyms as $synonym) {
        printf(
            "      %-20s = %-12s %s%s",
            $synonym->name,
            $synonym->value ?? '<null>',
            ConfigSource::nameOf($synonym->source),
            PHP_EOL
        );
    }
}

// ownValues() is the whole point of the "replaces everything" rule: read what the topic really has, change one
// option, and send the result back. Without it `retention.ms` alone would reset every other option of the topic.
$current = $admin->describeConfigs([$topicResource])[$topicKey]->ownValues();
$altered = $admin->alterConfigs([$topicKey => ['retention.ms' => '3600000'] + $current]);

echo "\nalterConfigs(): " . ($altered[$topicKey] === null
    ? 'ok, retention.ms is now ' . $admin->describeConfigs([$topicResource], ['retention.ms'])[$topicKey]->value('retention.ms')
    : 'error ' . $altered[$topicKey]->getCode() . ' - ' . $altered[$topicKey]->getMessage()) . "\n";

// Nothing is thrown for a resource that was refused: the result has one entry per resource, exactly like
// createTopics(). A 1.1 broker takes a broker resource, but only for the options KIP-226 made dynamic - a static
// one is answered with 42 and the names it cannot update at runtime.
$brokerKey  = ConfigResource::broker(0)->key();
$refused    = $admin->alterConfigs([$brokerKey => ['log.retention.hours' => '1']]);
echo 'A static broker option is refused with the code ' . ($refused[$brokerKey]?->getCode() ?? 0) . ': '
    . ($refused[$brokerKey]?->getContext()['error'] ?? 'accepted?!') . "\n";

// A dynamic one is applied at runtime and reported with the source DYNAMIC_BROKER_CONFIG afterwards; the same
// option on ConfigResource::defaultBroker() - the resource type 4 with an empty name - is the cluster-wide
// default that every broker picks up, with the source DYNAMIC_DEFAULT_BROKER_CONFIG.
//
//   $admin->alterConfigs([$brokerKey => ['log.cleaner.backoff.ms' => '16000']]);
//   $admin->alterConfigs([ConfigResource::defaultBroker()->key() => ['log.cleaner.threads' => '2']]);
//
// The example does not change the broker it is run against; it only reads it.

// The live KafkaConfig of a broker, which only that broker can answer. Since KIP-226 an entry is read-only when it
// is NOT dynamically updatable, and the value of a sensitive option (a password) is never sent and arrives as null
$brokerConfig = $admin->describeConfigs(
    [ConfigResource::broker(0)],
    ['log.retention.hours', 'num.partitions', 'log.cleaner.backoff.ms', 'ssl.key.password'],
    true
);
echo "\nConfiguration of broker 0\n";
foreach ($brokerConfig[$brokerKey]->entries as $entry) {
    printf(
        "  %-24s = %-12s %s%s%s",
        $entry->name,
        $entry->value ?? '<null>',
        ConfigSource::nameOf($entry->source),
        $entry->isReadOnly ? ', not dynamically updatable' : '',
        PHP_EOL
    );
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
