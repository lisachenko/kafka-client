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
use Protocol\Kafka\Common\NodeV0;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\TopicMetadataV0;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequestV0;
use Protocol\Kafka\Protocol\Request\MetadataRequestV1;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\MetadataResponseV0;
use Protocol\Kafka\Protocol\Request\MetadataResponseV1;

/**
 * Byte-exact tests of the Metadata API, versions 0, 1 and 2.
 *
 * <pre>
 *   Metadata Request (Version: 0)      => [TopicName]
 *   Metadata Request (Version: 1, 2)   => [TopicName]                          # the array is nullable
 *   Metadata Response (Version: 0)     => [Broker][TopicMetadata]
 *   Metadata Response (Version: 1)     => [Broker] ControllerId [TopicMetadata]
 *   Metadata Response (Version: 2)     => [Broker] ClusterId ControllerId [TopicMetadata]
 *     Broker            => NodeId int32 Host string Port int32 [Rack nullable string]
 *     TopicMetadata     => TopicErrorCode int16 TopicName string [IsInternal boolean] [PartitionMetadata]
 *     PartitionMetadata => PartitionErrorCode int16 PartitionId int32 Leader int32 Replicas [int32] Isr [int32]
 * </pre>
 *
 * @see docs/protocol/0.11.0.md, section "Metadata API (key 3, v0, v1 and v2)"
 */
#[CoversClass(MetadataRequest::class)]
#[CoversClass(MetadataRequestV0::class)]
#[CoversClass(MetadataRequestV1::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(MetadataResponseV0::class)]
#[CoversClass(MetadataResponseV1::class)]
#[CoversClass(Node::class)]
#[CoversClass(NodeV0::class)]
#[CoversClass(TopicMetadata::class)]
#[CoversClass(TopicMetadataV0::class)]
#[CoversClass(PartitionMetadata::class)]
final class MetadataApiTest extends TestCase
{
    /**
     * A version 0 response that announces two brokers and one topic with three partitions.
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
     * The same cluster as a version 2 answer: every broker carries a Rack, every topic an IsInternal flag, and the
     * ClusterId and the ControllerId sit between the two arrays - in that order.
     *
     *   Size            => 00 00 00 b6 (182 bytes)
     *   [Broker]        => 0, "kafka-1", 9092, rack "eu-1"; 1, "kafka-2", 9093, rack null
     *   ClusterId       => "cluster-a"
     *   ControllerId    => 1
     *   [TopicMetadata] => error 0, "orders", not internal, three partitions
     */
    private const string CLUSTER_RESPONSE_V2_HEX = '000000b6'
        . '0000002a'
        . '00000002'
        . '00000000' . '0007' . '6b61666b612d31' . '00002384' . '0004' . '65752d31'
        . '00000001' . '0007' . '6b61666b612d32' . '00002385' . 'ffff'
        . '0009' . '636c75737465722d61'
        . '00000001'
        . '00000001'
        . '0000' . '0006' . '6f7264657273' . '00' . '00000003'
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
        //   Size => 18, ApiKey 3, ApiVersion 2, CorrelationId 1, ClientId "test", [TopicName] => null
        $request = new MetadataRequest(null, 'test', 1);

        self::assertSame(
            '00000012' . '0003' . '0002' . '00000001' . '0004' . '74657374' . 'ffffffff',
            bin2hex((string) $request)
        );
        self::assertNull($request->getTopics(), 'a null topic array is the "every topic" of version 1 and above');
    }

    public function testRequestWithAnEmptyTopicArrayAsksForNoTopicAtAll(): void
    {
        // The very frame version 0 uses for "every topic" means "no topic" from version 1 on
        $request = new MetadataRequest([], 'test', 1);

        self::assertSame(
            '00000012' . '0003' . '0002' . '00000001' . '0004' . '74657374' . '00000000',
            bin2hex((string) $request)
        );
        self::assertSame([], $request->getTopics());
    }

    public function testRequestPacksEveryRequestedTopicAsAString(): void
    {
        //   Size => 41, ClientId "php-kafka", [TopicName] => "orders", "payments"
        $request = new MetadataRequest(['orders', 'payments'], 'php-kafka', 7);

        self::assertSame(
            '00000029' . '0003' . '0002' . '00000007' . '0009' . '7068702d6b61666b61'
            . '00000002' . '0006' . '6f7264657273' . '0008' . '7061796d656e7473',
            bin2hex((string) $request)
        );
        self::assertSame(['orders', 'payments'], $request->getTopics());
    }

    public function testVersionOneSendsTheSameFrameAsVersionTwo(): void
    {
        // METADATA_REQUEST_V2 = METADATA_REQUEST_V1, only the version field of the header differs
        $version1 = bin2hex((string) new MetadataRequestV1(['orders'], 'test', 3));
        $version2 = bin2hex((string) new MetadataRequest(['orders'], 'test', 3));

        self::assertSame(str_replace('00030001', '00030002', $version1), $version2);
        self::assertSame(1, new MetadataRequestV1()->getApiVersion());
        self::assertSame(2, new MetadataRequest()->getApiVersion());
    }

    public function testRequestTopicsAreNotNullableInVersionZero(): void
    {
        // The nullable topic array only arrived with version 1 of this API (Kafka 0.10.0)
        self::assertSame([BinarySchema::TYPE_STRING], MetadataRequestV0::getScheme()['topics']);
        self::assertSame(
            [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true],
            MetadataRequest::getScheme()['topics']
        );
        self::assertSame(0, new MetadataRequestV0()->getApiVersion());
    }

    public function testVersionZeroWritesANullTopicArrayAsAnEmptyOne(): void
    {
        // Version 0 has no way to say "no topic", so both intentions are the same frame there
        $request = new MetadataRequestV0(null, 'test', 1);

        self::assertSame(
            '00000012' . '0003' . '0000' . '00000001' . '0004' . '74657374' . '00000000',
            bin2hex((string) $request)
        );
        self::assertSame([], $request->getTopics());
    }

    public function testResponseIsDecodedIntoBrokersTopicsAndPartitions(): void
    {
        $response = MetadataResponseV0::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_HEX)));

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

    public function testVersionZeroAnswerCarriesNeitherClusterIdNorControllerIdNorRackNorInternalFlag(): void
    {
        $response = MetadataResponseV0::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_HEX)));

        self::assertNull($response->clusterId);
        self::assertNull($response->controllerId);
        self::assertInstanceOf(NodeV0::class, $response->brokers[0]);
        self::assertNull($response->brokers[0]->rack);
        self::assertInstanceOf(TopicMetadataV0::class, $response->topics['orders']);
        self::assertNull(
            $response->topics['orders']->isInternal,
            'null is "the answer did not say", not "the topic is not internal"'
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'brokers', 'topics'],
            array_keys(MetadataResponseV0::getScheme())
        );
    }

    public function testVersionTwoAnswerCarriesTheClusterIdBeforeTheControllerId(): void
    {
        $response = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)));

        self::assertSame(
            ['messageSize', 'correlationId', 'brokers', 'clusterId', 'controllerId', 'topics'],
            array_keys(MetadataResponse::getScheme()),
            'version 2 inserts the cluster id BEFORE the controller id'
        );
        self::assertSame('cluster-a', $response->clusterId);
        self::assertSame(1, $response->controllerId);
        self::assertSame('eu-1', $response->brokers[0]->rack);
        self::assertNull($response->brokers[1]->rack, 'a broker without broker.rack answers a null string');
        self::assertFalse($response->topics['orders']->isInternal);
        self::assertSame([0, 1, 2], array_keys($response->topics['orders']->partitions));
    }

    public function testVersionOneAnswerHasTheControllerIdButNoClusterId(): void
    {
        // The same frame without the ClusterId: the field arrived one release later than the rest
        $frame = hex2bin(
            '00000049'
            . '0000002a'
            . '00000002'
            . '00000000' . '0007' . '6b61666b612d31' . '00002384' . '0004' . '65752d31'
            . '00000001' . '0007' . '6b61666b612d32' . '00002385' . 'ffff'
            . '00000001'
            . '00000001'
            . '0000' . '0006' . '6f7264657273' . '00' . '00000000'
        );

        $response = MetadataResponseV1::unpack(new StringStream($frame));

        self::assertSame(
            ['messageSize', 'correlationId', 'brokers', 'controllerId', 'topics'],
            array_keys(MetadataResponseV1::getScheme())
        );
        self::assertNull($response->clusterId);
        self::assertSame(1, $response->controllerId);
        self::assertSame('eu-1', $response->brokers[0]->rack);
        self::assertFalse($response->topics['orders']->isInternal);
    }

    public function testAnInternalTopicIsDecodedAsSuch(): void
    {
        //   [Broker] => none, ClusterId "c", ControllerId 0, one internal topic without partitions
        $frame = hex2bin(
            '0000002e'
            . '0000002a'
            . '00000000'
            . '0001' . '63'
            . '00000000'
            . '00000001'
            . '0000' . '0012' . '5f5f636f6e73756d65725f6f666673657473' . '01' . '00000000'
        );

        $response = MetadataResponse::unpack(new StringStream($frame));

        self::assertTrue($response->topics['__consumer_offsets']->isInternal);
    }

    public function testControllerIdIsMinusOneWhileTheClusterElectsAController(): void
    {
        //   [Broker] => none, ClusterId null, ControllerId -1, no topics
        $frame = hex2bin('00000012' . '0000002a' . '00000000' . 'ffff' . 'ffffffff' . '00000000');

        $response = MetadataResponse::unpack(new StringStream($frame));

        self::assertNull($response->clusterId, 'a broker without a cluster id answers a null string');
        self::assertSame(MetadataResponse::NO_CONTROLLER_ID, $response->controllerId);
        self::assertSame(-1, MetadataResponse::NO_CONTROLLER_ID);
    }

    public function testResponseKeepsTheReplicaAndIsrSetsOfEveryPartition(): void
    {
        $partitions = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)))
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
        $response = MetadataResponseV0::unpack(new StringStream(hex2bin(self::LEADER_NOT_AVAILABLE_RESPONSE_HEX)));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame([0], array_keys($response->brokers));

        $topic = $response->topics['new-topic'];
        self::assertSame(5, $topic->topicErrorCode, 'LeaderNotAvailable, the topic is still being created');
        self::assertSame('new-topic', $topic->topic);
        self::assertSame([], $topic->partitions);
    }

    public function testResponseArraysAreIndexedByTheFieldTheClusterLooksThemUpBy(): void
    {
        $response = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)));

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
        $response = MetadataResponse::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)));

        /** @var MetadataResponse $restored */
        $restored = eval('return ' . var_export($response, true) . ';');

        self::assertInstanceOf(MetadataResponse::class, $restored);
        self::assertEquals($response->brokers, $restored->brokers);
        self::assertEquals($response->topics, $restored->topics);
        self::assertSame('cluster-a', $restored->clusterId, 'the cluster id survives the cache file');
        self::assertSame(1, $restored->controllerId, 'and so does the controller id');
        self::assertInstanceOf(Node::class, $restored->brokers[1]);
        self::assertSame('eu-1', $restored->brokers[0]->rack);
        self::assertInstanceOf(TopicMetadata::class, $restored->topics['orders']);
        self::assertFalse($restored->topics['orders']->isInternal);
        self::assertInstanceOf(PartitionMetadata::class, $restored->topics['orders']->partitions[2]);
        self::assertSame([1, 0], $restored->topics['orders']->partitions[2]->isr);
    }
}
