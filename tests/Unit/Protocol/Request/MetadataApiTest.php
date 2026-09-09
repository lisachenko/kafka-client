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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

/**
 * Byte-exact tests of the Metadata API v0.
 *
 * <pre>
 *   TopicMetadataRequest => [TopicName]
 *   MetadataResponse     => [Broker][TopicMetadata]
 *     Broker            => NodeId int32 Host string Port int32
 *     TopicMetadata     => TopicErrorCode int16 TopicName string [PartitionMetadata]
 *     PartitionMetadata => PartitionErrorCode int16 PartitionId int32 Leader int32 Replicas [int32] Isr [int32]
 * </pre>
 *
 * @see docs/protocol/0.10.2.md, section "Metadata API (key 3, v0)"
 */
#[CoversClass(MetadataRequest::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(Node::class)]
#[CoversClass(TopicMetadata::class)]
#[CoversClass(PartitionMetadata::class)]
final class MetadataApiTest extends TestCase
{
    /**
     * A response that announces two brokers and one topic with three partitions.
     *
     *   Size               => 00 00 00 9e (158 bytes)
     *   CorrelationId      => 00 00 00 2a
     *   [Broker]           => 00 00 00 02
     *     0, "kafka-1", 9092 and 1, "kafka-2", 9093
     *   [TopicMetadata]    => 00 00 00 01
     *     TopicErrorCode 0, "orders", three partitions with their replicas and in-sync replicas
     */
    private const string CLUSTER_RESPONSE_HEX = '0000009e'
        . '0000002a'
        . '00000002'
        . '00000000' . '0007' . '6b61666b612d31' . '00002384'
        . '00000001' . '0007' . '6b61666b612d32' . '00002385'
        . '00000001'
        . '0000' . '0006' . '6f7264657273' . '00000003'
        . '0000' . '00000000' . '00000000' . '00000002' . '00000000' . '00000001'
        . '00000002' . '00000000' . '00000001'
        . '0000' . '00000001' . '00000001' . '00000002' . '00000001' . '00000000'
        . '00000001' . '00000001'
        . '0000' . '00000002' . '00000001' . '00000002' . '00000001' . '00000000'
        . '00000002' . '00000001' . '00000000';

    /**
     * The answer for a topic that has just been auto-created: error 5 (LeaderNotAvailable), no partitions at all.
     *
     *   Size            => 00 00 00 2e (46 bytes)
     *   CorrelationId   => 00 00 00 07
     *   [Broker]        => 00 00 00 01, node 0 at "kafka-1:9092"
     *   [TopicMetadata] => 00 00 00 01, error 5, "new-topic", no partitions
     */
    private const string LEADER_NOT_AVAILABLE_RESPONSE_HEX = '0000002e'
        . '00000007'
        . '00000001' . '00000000' . '0007' . '6b61666b612d31' . '00002384'
        . '00000001' . '0005' . '0009' . '6e65772d746f706963' . '00000000';

    public function testRequestWithoutTopicsAsksForEveryTopic(): void
    {
        //   Size => 18, ApiKey 3, ApiVersion 0, CorrelationId 1, ClientId "test", [TopicName] => empty
        $request = new MetadataRequest([], 'test', 1);

        self::assertSame(
            '00000012' . '0003' . '0000' . '00000001' . '0004' . '74657374' . '00000000',
            bin2hex((string) $request)
        );
        self::assertSame([], $request->getTopics());
    }

    public function testRequestPacksEveryRequestedTopicAsAString(): void
    {
        //   Size => 41, ClientId "php-kafka", [TopicName] => "orders", "payments"
        $request = new MetadataRequest(['orders', 'payments'], 'php-kafka', 7);

        self::assertSame(
            '00000029' . '0003' . '0000' . '00000007' . '0009' . '7068702d6b61666b61'
            . '00000002' . '0006' . '6f7264657273' . '0008' . '7061796d656e7473',
            bin2hex((string) $request)
        );
        self::assertSame(['orders', 'payments'], $request->getTopics());
    }

    public function testRequestTopicsAreNotNullableInVersionZero(): void
    {
        // The nullable topic array only arrived with version 1 of this API (Kafka 0.10.0)
        self::assertSame([BinarySchema::TYPE_STRING], MetadataRequest::getScheme()['topics']);
        self::assertSame(0, new MetadataRequest()->getApiVersion());
    }

    public function testResponseIsDecodedIntoBrokersTopicsAndPartitions(): void
    {
        $response = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_HEX)));

        self::assertSame(42, $response->getCorrelationId());
        self::assertSame([0, 1], array_keys($response->brokers));
        self::assertSame('kafka-1', $response->brokers[0]->host);
        self::assertSame(9092, $response->brokers[0]->port);
        self::assertSame(1, $response->brokers[1]->nodeId);
        self::assertSame('kafka-2', $response->brokers[1]->host);
        self::assertSame(9093, $response->brokers[1]->port);

        self::assertSame(['orders'], array_keys($response->topics));
        $topic = $response->topics['orders'];
        self::assertSame(0, $topic->topicErrorCode);
        self::assertSame([0, 1, 2], array_keys($topic->partitions));
    }

    public function testResponseKeepsTheReplicaAndIsrSetsOfEveryPartition(): void
    {
        $partitions = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_HEX)))
            ->topics['orders']
            ->partitions;

        self::assertSame(0, $partitions[0]->leader);
        self::assertSame([0, 1], $partitions[0]->replicas);
        self::assertSame([0, 1], $partitions[0]->isr);

        self::assertSame(1, $partitions[1]->leader);
        self::assertSame([1, 0], $partitions[1]->replicas);
        self::assertSame([1], $partitions[1]->isr, 'the replica 0 is out of sync for this partition');

        self::assertSame(2, $partitions[2]->partitionId);
        self::assertSame(1, $partitions[2]->leader);
        self::assertSame([1, 0], $partitions[2]->replicas);
        self::assertSame([1, 0], $partitions[2]->isr);
    }

    public function testTopicWithAnErrorCodeAndWithoutPartitionsIsDecoded(): void
    {
        $response = MetadataResponse::unpack(new StringStream(hex2bin(self::LEADER_NOT_AVAILABLE_RESPONSE_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame([0], array_keys($response->brokers));

        $topic = $response->topics['new-topic'];
        self::assertSame(5, $topic->topicErrorCode, 'LeaderNotAvailable, the topic is still being created');
        self::assertSame('new-topic', $topic->topic);
        self::assertSame([], $topic->partitions);
    }

    public function testResponseArraysAreIndexedByTheFieldTheClusterLooksThemUpBy(): void
    {
        $response = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_HEX)));

        foreach ($response->brokers as $nodeId => $broker) {
            self::assertSame($nodeId, $broker->nodeId);
        }
        foreach ($response->topics as $topicName => $topic) {
            self::assertSame($topicName, $topic->topic);
            foreach ($topic->partitions as $partitionId => $partition) {
                self::assertSame($partitionId, $partition->partitionId);
            }
        }
    }

    public function testResponseSurvivesTheMetadataCacheFile(): void
    {
        // Cluster stores the whole response with var_export() and includes it back, see Cluster::reload()
        $response = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_HEX)));

        /** @var MetadataResponse $restored */
        $restored = eval('return ' . var_export($response, true) . ';');

        self::assertInstanceOf(MetadataResponse::class, $restored);
        self::assertEquals($response->brokers, $restored->brokers);
        self::assertEquals($response->topics, $restored->topics);
        self::assertInstanceOf(Node::class, $restored->brokers[1]);
        self::assertInstanceOf(TopicMetadata::class, $restored->topics['orders']);
        self::assertInstanceOf(PartitionMetadata::class, $restored->topics['orders']->partitions[2]);
        self::assertSame([1, 0], $restored->topics['orders']->partitions[2]->isr);
    }
}
