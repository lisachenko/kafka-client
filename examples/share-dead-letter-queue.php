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
 * The dead-letter queue of a **share group** (KIP-1191, Kafka 4.4, the feature `share.version` 2).
 *
 * A record that a member REJECTs, or that reaches the delivery count limit of the group, is written by the broker
 * into the DLQ topic the group config `errors.deadletterqueue.topic.name` names, before the broker archives it. There
 * is nothing new on the wire and nothing to do in the consumer: the example creates a DLQ topic with the topic config
 * `errors.deadletterqueue.group.enable=true` and a name with the cluster's prefix `dlq.`, points the group at it,
 * rejects every record it is delivered and reads the DLQ topic back. With `--copy` the group config
 * `errors.deadletterqueue.copy.record.enable` is set too, and the DLQ records carry the key and the value of the
 * source record; without it they carry the `__dlq.errors.*` headers alone.
 *
 *   docker compose up -d
 *   php examples/share-dead-letter-queue.php
 *   php examples/share-dead-letter-queue.php --copy
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 php examples/share-dead-letter-queue.php
 *
 * @see docs/protocol/overview.md, section "The dead-letter queue of KIP-1191 (share.version 2)"
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\AcknowledgeType;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\KafkaShareConsumer;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;

require_once __DIR__ . '/../vendor/autoload.php';

$bootstrapServers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9092';
$suffix           = bin2hex(random_bytes(3));
$topic            = "example-dlq-source-{$suffix}";
$dlqTopic         = "dlq.example-dlq-{$suffix}";
$group            = "example-dlq-group-{$suffix}";
$copyRecords      = in_array('--copy', $argv, true);

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . $bootstrapServers],
    ClientConfig::CLIENT_ID          => 'example-share-dlq',
    ClientConfig::REQUEST_TIMEOUT_MS => 30000,
    ProducerConfig::ACKS             => 1,
];
$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

// 1. The source topic and the DLQ topic: the name starts with `dlq.`, and the topic is enabled as a DLQ
$admin->createTopics([
    new NewTopic($topic, 1, 1),
    new NewTopic($dlqTopic, 1, 1, configs: ['errors.deadletterqueue.group.enable' => 'true']),
]);
sleep(1);
$producer = new KafkaProducer($configuration);
for ($index = 0; $index < 3; $index++) {
    $producer->send($topic, new Record("value-{$index}", "key-{$index}"), 0);
}
$producer->flush();
echo "Three records written into {$topic}\n";

// 2. The group configs, on the config resource type 32 of KIP-848 and KIP-932
$groupConfigs = ['share.auto.offset.reset' => 'earliest', 'errors.deadletterqueue.topic.name' => $dlqTopic];
if ($copyRecords) {
    $groupConfigs['errors.deadletterqueue.copy.record.enable'] = 'true';
}
$stream = new SocketStream('tcp://' . $bootstrapServers, $configuration, 5.0);
foreach ($groupConfigs as $name => $value) {
    new IncrementalAlterConfigsRequest(
        [new IncrementalAlterConfigsRequestResource(32, $group, [AlterConfigOp::set($name, $value)])],
        false,
        'example-share-dlq'
    )->writeTo($stream);
    $answer = IncrementalAlterConfigsResponse::unpack($stream)->responses[0];
    if ($answer->errorCode !== 0) {
        throw KafkaException::fromCode($answer->errorCode, ['config' => $name, 'error' => $answer->errorMessage]);
    }
}
echo "Share group {$group} writes its dead letters into {$dlqTopic}\n";

// 3. A member that rejects every record it is delivered
$consumer = new KafkaShareConsumer($configuration + [
    ConsumerConfig::GROUP_ID                   => $group,
    ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE => ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_EXPLICIT,
]);
$consumer->subscribe([$topic]);
$rejected = 0;
for ($attempt = 0; $attempt < 10 && $rejected < 3; $attempt++) {
    foreach ($consumer->poll(2000) as $partitions) {
        foreach ($partitions as $records) {
            foreach ($records as $record) {
                $consumer->acknowledge($record, AcknowledgeType::REJECT);
                $rejected++;
            }
        }
    }
}
$consumer->commitSync();
$consumer->close();
echo "Rejected {$rejected} records\n\n";

// 4. The DLQ topic, read by a plain consumer: the broker writes it after it answered the acknowledgements
$reader = new KafkaConsumer($configuration + [
    ConsumerConfig::GROUP_ID           => "example-dlq-reader-{$suffix}",
    ConsumerConfig::ENABLE_AUTO_COMMIT => false,
]);
$reader->assign([$dlqTopic => [0]]);
$reader->seekToBeginning([$dlqTopic => [0]]);
$read = 0;
for ($attempt = 0; $attempt < 10 && $read < $rejected; $attempt++) {
    foreach ($reader->poll(1000) as $partitions) {
        foreach ($partitions as $records) {
            foreach ($records as $record) {
                $read++;
                printf("%s@%d key=%s value=%s\n", $dlqTopic, (int) $record->offset, $record->key ?? 'null', $record->value ?? 'null');
                foreach ($record->headers as $header) {
                    printf("  %-28s %s\n", $header->key, (string) $header->value);
                }
            }
        }
    }
}
$reader->close();

// 5. Clean up
$admin->deleteConsumerGroups([$group]);
$admin->deleteTopics([$topic, $dlqTopic]);
