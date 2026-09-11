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
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\NodeV0;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\PartitionMetadataV0;
use Protocol\Kafka\Common\PartitionMetadataV5;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Common\TopicMetadataV0;
use Protocol\Kafka\Common\TopicMetadataV1;
use Protocol\Kafka\Common\TopicMetadataV5;
use Protocol\Kafka\Common\TopicMetadataV7;
use Protocol\Kafka\Common\TopicMetadataV8;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\MetadataRequestTopic;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequestV0;
use Protocol\Kafka\Protocol\Request\MetadataRequestV1;
use Protocol\Kafka\Protocol\Request\MetadataRequestV10;
use Protocol\Kafka\Protocol\Request\MetadataRequestV2;
use Protocol\Kafka\Protocol\Request\MetadataRequestV3;
use Protocol\Kafka\Protocol\Request\MetadataRequestV4;
use Protocol\Kafka\Protocol\Request\MetadataRequestV5;
use Protocol\Kafka\Protocol\Request\MetadataRequestV6;
use Protocol\Kafka\Protocol\Request\MetadataRequestV7;
use Protocol\Kafka\Protocol\Request\MetadataRequestV8;
use Protocol\Kafka\Protocol\Request\MetadataRequestV9;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\MetadataResponseV0;
use Protocol\Kafka\Protocol\Request\MetadataResponseV1;
use Protocol\Kafka\Protocol\Request\MetadataResponseV2;
use Protocol\Kafka\Protocol\Request\MetadataResponseV3;
use Protocol\Kafka\Protocol\Request\MetadataResponseV4;
use Protocol\Kafka\Protocol\Request\MetadataResponseV5;
use Protocol\Kafka\Protocol\Request\MetadataResponseV6;
use Protocol\Kafka\Protocol\Request\MetadataResponseV7;
use Protocol\Kafka\Protocol\Request\MetadataResponseV8;
use Protocol\Kafka\Protocol\Request\MetadataResponseV9;

/**
 * Byte-exact tests of the Metadata API, versions 0 to 5.
 *
 * <pre>
 *   Metadata Request (Version: 0)        => [TopicName]
 *   Metadata Request (Version: 1, 2, 3)  => [TopicName]                        # the array is nullable
 *   Metadata Request (Version: 4, 5)     => [TopicName] AllowAutoTopicCreation
 *   Metadata Response (Version: 0)       => [Broker][TopicMetadata]
 *   Metadata Response (Version: 1)       => [Broker] ControllerId [TopicMetadata]
 *   Metadata Response (Version: 2)       => [Broker] ClusterId ControllerId [TopicMetadata]
 *   Metadata Response (Version: 3, 4, 5) => ThrottleTimeMs [Broker] ClusterId ControllerId [TopicMetadata]
 *     Broker            => NodeId int32 Host string Port int32 [Rack nullable string]
 *     TopicMetadata     => TopicErrorCode int16 TopicName string [IsInternal boolean] [PartitionMetadata]
 *     PartitionMetadata => PartitionErrorCode int16 PartitionId int32 Leader int32 Replicas [int32] Isr [int32]
 *                          [OfflineReplicas [int32]]      # since version 5
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
#[CoversClass(MetadataRequest::class)]
#[CoversClass(MetadataRequestV0::class)]
#[CoversClass(MetadataRequestV1::class)]
#[CoversClass(MetadataRequestV2::class)]
#[CoversClass(MetadataRequestV4::class)]
#[CoversClass(MetadataRequestV3::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(MetadataResponseV0::class)]
#[CoversClass(MetadataResponseV1::class)]
#[CoversClass(MetadataResponseV2::class)]
#[CoversClass(MetadataResponseV4::class)]
#[CoversClass(MetadataResponseV3::class)]
#[CoversClass(Node::class)]
#[CoversClass(NodeV0::class)]
#[CoversClass(TopicMetadata::class)]
#[CoversClass(TopicMetadataV1::class)]
#[CoversClass(TopicMetadataV0::class)]
#[CoversClass(PartitionMetadata::class)]
#[CoversClass(PartitionMetadataV0::class)]
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

    /**
     * A version 5 answer with one broker and the topic "orders", whose only partition has an offline replica.
     *
     *   Size            => 00 00 00 5e (94 bytes), CorrelationId => 00 00 00 2a
     *   ThrottleTimeMs  => 00 00 00 00
     *   [Broker]        => 00 00 00 01, node 0 at "kafka-1:9092", Rack => ff ff (null)
     *   ClusterId       => ff ff (null), ControllerId => 00 00 00 00
     *   [TopicMetadata] => 00 00 00 01, error 0, "orders", IsInternal => 00, one partition:
     *     error 0, id 0, leader 0, Replicas [0, 1], Isr [0], OfflineReplicas [1]
     */
    private const string OFFLINE_REPLICAS_RESPONSE_V5_HEX = '0000005e' . '0000002a' . '00000000'
        . '00000001' . '00000000' . '0007' . '6b61666b612d31' . '00002384' . 'ffff'
        . 'ffff' . '00000000'
        . '00000001' . '0000' . '0006' . '6f7264657273' . '00'
        . '00000001' . '0000' . '00000000' . '00000000'
        . '00000002' . '00000000' . '00000001'
        . '00000001' . '00000000'
        . '00000001' . '00000001';

    public function testRequestWithoutTopicsAsksForEveryTopic(): void
    {
        //   Size => 21, ApiKey 3, ApiVersion 8, CorrelationId 1, ClientId "test", [TopicName] => null,
        //   allow => 01, and the two booleans of KIP-430, both off
        $request = new MetadataRequestV8(null, true, 'test', 1);

        self::assertSame(
            '00000015' . '0003' . '0008' . '00000001' . '0004' . '74657374' . 'ffffffff' . '01' . '00' . '00',
            bin2hex((string) $request)
        );
        self::assertNull($request->getTopics(), 'a null topic array is the "every topic" of version 1 and above');
    }

    public function testRequestWithAnEmptyTopicArrayAsksForNoTopicAtAll(): void
    {
        // The very frame version 0 uses for "every topic" means "no topic" from version 1 on
        $request = new MetadataRequestV8([], true, 'test', 1);

        self::assertSame(
            '00000015' . '0003' . '0008' . '00000001' . '0004' . '74657374' . '00000000' . '01' . '00' . '00',
            bin2hex((string) $request)
        );
        self::assertSame([], $request->getTopics());
    }

    public function testRequestPacksEveryRequestedTopicAsAString(): void
    {
        //   Size => 44, ClientId "php-kafka", [TopicName] => "orders", "payments"
        $request = new MetadataRequestV8(['orders', 'payments'], true, 'php-kafka', 7);

        self::assertSame(
            '0000002c' . '0003' . '0008' . '00000007' . '0009' . '7068702d6b61666b61'
            . '00000002' . '0006' . '6f7264657273' . '0008' . '7061796d656e7473' . '01' . '00' . '00',
            bin2hex((string) $request)
        );
        self::assertSame(['orders', 'payments'], $request->getTopics());
    }

    public function testTheAutoCreationFlagIsTheLastByteOfAVersionFourAndFiveFrame(): void
    {
        // The flag is the last byte up to version 7; version 8 (KIP-430) put the two authorized-operation
        // booleans behind it, so it is the third byte from the end of the frame this client sends
        $allowed = bin2hex((string) new MetadataRequestV7(['orders'], true, 'test', 3));
        $refused = bin2hex((string) new MetadataRequestV7(['orders'], false, 'test', 3));

        self::assertStringEndsWith('01', $allowed);
        self::assertStringEndsWith('00', $refused);
        self::assertStringEndsWith(
            '01' . '00' . '00' . '00',
            bin2hex((string) new MetadataRequestV9(['orders'], true, 'test', 3)),
            'and a version 9 frame closes with the two booleans and the tagged-field section of the body'
        );
        self::assertStringEndsWith(
            '01' . '00' . '00',
            bin2hex((string) new MetadataRequest(['orders'], true, 'test', 3)),
            'a version 11 frame has only the topic boolean left: KIP-700 took the cluster one out'
        );
        self::assertSame(substr($allowed, 0, -2), substr($refused, 0, -2), 'the flag is the only difference');
        self::assertTrue(new MetadataRequest()->isAutoTopicCreationAllowed(), 'true is the behaviour of every older version');
    }

    public function testTheVersionsOneToThreeSendTheSameFrameWithoutTheAutoCreationFlag(): void
    {
        // METADATA_REQUEST_V3 = METADATA_REQUEST_V2 = METADATA_REQUEST_V1, only the version field differs
        $version1 = bin2hex((string) new MetadataRequestV1(['orders'], 'test', 3));
        $version2 = bin2hex((string) new MetadataRequestV2(['orders'], 'test', 3));
        $version3 = bin2hex((string) new MetadataRequestV3(['orders'], 'test', 3));

        self::assertSame(str_replace('00030001', '00030002', $version1), $version2);
        self::assertSame(str_replace('00030001', '00030003', $version1), $version3);
        self::assertSame(1, new MetadataRequestV1()->getApiVersion());
        self::assertSame(2, new MetadataRequestV2()->getApiVersion());
        self::assertSame(3, new MetadataRequestV3()->getApiVersion());
        self::assertSame(4, new MetadataRequestV4()->getApiVersion());
        self::assertSame(5, new MetadataRequestV5()->getApiVersion());
        self::assertSame(6, new MetadataRequestV6()->getApiVersion());
        self::assertSame(7, new MetadataRequestV7()->getApiVersion());
        self::assertSame(8, new MetadataRequestV8()->getApiVersion());
        self::assertSame(9, new MetadataRequestV9()->getApiVersion());
        self::assertSame(10, new MetadataRequestV10()->getApiVersion());
        self::assertSame(11, new MetadataRequest()->getApiVersion());
        self::assertArrayNotHasKey('allowAutoTopicCreation', MetadataRequestV3::getScheme());
        self::assertArrayHasKey('allowAutoTopicCreation', MetadataRequest::getScheme());
    }

    public function testTheVersionsFourToSevenSendOneAndTheSameFrame(): void
    {
        // `MetadataRequest.json` @ 2.8.2 has no field between version 4 and version 8: version 5 (Kafka 1.0)
        // states that the client understands the `offline_replicas` array of the ANSWER and version 6 (Kafka 2.0,
        // KIP-219) that it waits out `throttle_time_ms` itself - only the api version of the header says so
        $version4 = bin2hex((string) new MetadataRequestV4(['orders'], true, 'test', 3));
        $version5 = bin2hex((string) new MetadataRequestV5(['orders'], true, 'test', 3));
        $version6 = bin2hex((string) new MetadataRequestV6(['orders'], true, 'test', 3));
        $version7 = bin2hex((string) new MetadataRequestV7(['orders'], true, 'test', 3));

        self::assertSame('0004', substr($version4, 12, 4), 'the api version sits behind Size and ApiKey');
        self::assertSame('0005', substr($version5, 12, 4));
        self::assertSame('0006', substr($version6, 12, 4));
        self::assertSame('0007', substr($version7, 12, 4));
        self::assertSame(substr_replace($version4, '0005', 12, 4), $version5);
        self::assertSame(substr_replace($version4, '0006', 12, 4), $version6);
        self::assertSame(substr_replace($version4, '0007', 12, 4), $version7);
        self::assertSame(MetadataRequestV4::getScheme(), MetadataRequestV7::getScheme());
        self::assertSame(MetadataResponseV5::getScheme(), MetadataResponseV6::getScheme());
        self::assertSame(7, MetadataRequestV7::VERSION);
        self::assertSame(7, MetadataResponseV7::VERSION);
        self::assertSame(8, MetadataRequestV8::VERSION);
        self::assertSame(8, MetadataResponseV8::VERSION);
        self::assertSame(9, MetadataRequestV9::VERSION);
        self::assertSame(9, MetadataResponseV9::VERSION);
        self::assertSame(11, MetadataRequest::VERSION);
        self::assertSame(11, MetadataResponse::VERSION);
    }

    public function testVersionEightAsksForTheAuthorizedOperationsAndIsAnsweredTwoBitfields(): void
    {
        // KIP-430 (Kafka 2.3): two booleans behind `allow_auto_topic_creation`, in the order of
        // `MetadataRequest.json` @ 2.8.2 - the cluster one first, the topic one second - and two int32 bitfields
        // in the answer: one at the end of every topic entry, one at the end of the frame
        $asking = bin2hex((string) new MetadataRequestV8(['orders'], false, 'test', 3, true, true));
        $silent = bin2hex((string) new MetadataRequestV8(['orders'], false, 'test', 3));

        self::assertStringEndsWith('00' . '01' . '01', $asking, 'no auto creation, both bitfields asked for');
        self::assertStringEndsWith('00' . '00' . '00', $silent, 'and neither of them by default');
        self::assertSame(substr($asking, 0, -4), substr($silent, 0, -4));
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'topics', 'allowAutoTopicCreation',
                'includeClusterAuthorizedOperations', 'includeTopicAuthorizedOperations'],
            array_keys(MetadataRequestV8::getScheme())
        );
        self::assertArrayNotHasKey('includeTopicAuthorizedOperations', MetadataRequestV7::getScheme());

        // The topic entry ends with its own bitfield, behind the partitions
        self::assertSame(
            ['topicErrorCode', 'topic', 'isInternal', 'partitions', 'authorizedOperations'],
            array_keys(TopicMetadataV8::getScheme())
        );
        self::assertSame(
            ['topicErrorCode', 'topic', 'topicId', 'isInternal', 'partitions', 'authorizedOperations'],
            array_keys(TopicMetadata::getScheme()),
            'the topic id of KIP-516 sits between the name and the internal flag from version 10 on'
        );
        self::assertSame(
            ['topicErrorCode', 'topic', 'isInternal', 'partitions'],
            array_keys(TopicMetadataV7::getScheme()),
            'the bitfield of KIP-430 arrived with version 8'
        );
        self::assertSame(AclOperation::NOT_REQUESTED, new TopicMetadata()->authorizedOperations);
        self::assertSame(AclOperation::NOT_REQUESTED, new MetadataResponse()->clusterAuthorizedOperations);
        self::assertSame(-2147483648, AclOperation::NOT_REQUESTED);
    }

    public function testVersionSevenInsertsTheLeaderEpochBehindTheLeaderOfEveryPartition(): void
    {
        // KIP-320: the field order of `MetadataResponse.json` @ 2.8.2 is the wire order, and it puts
        // `LeaderEpoch` between `LeaderId` and `ReplicaNodes`
        self::assertSame(
            ['partitionErrorCode', 'partitionId', 'leader', 'leaderEpoch', 'replicas', 'isr', 'offlineReplicas'],
            array_keys(PartitionMetadata::getScheme())
        );
        self::assertSame(
            ['partitionErrorCode', 'partitionId', 'leader', 'replicas', 'isr', 'offlineReplicas'],
            array_keys(PartitionMetadataV5::getScheme()),
            'the versions 5 and 6 carry the offline replicas and no epoch'
        );
        self::assertSame(
            ['partitionErrorCode', 'partitionId', 'leader', 'replicas', 'isr'],
            array_keys(PartitionMetadataV0::getScheme())
        );
        self::assertSame(-1, PartitionMetadata::UNKNOWN_LEADER_EPOCH);
        self::assertSame(
            ['topic' => TopicMetadataV5::class],
            MetadataResponseV6::getScheme()['topics'],
            'a version 6 answer packs the partition entries that carry no epoch'
        );
    }

    public function testVersionNineIsTheSameQuestionInTheFlexibleEncoding(): void
    {
        // KIP-482 (Kafka 2.4): the request header v2 (a tag buffer behind the client id), compact strings and
        // arrays, and a tagged-field section at the end of every structure. Not one field is added or moved
        $flexible = bin2hex((string) new MetadataRequestV9(['t2-24-flex'], false, 't2', 600));

        self::assertSame(
            '0000001e'
            . '0003' . '0009' . '00000258'
            . '0002' . '7432'                              // the client id is never compact (RequestHeader.json)
            . '00'                                         // the tag buffer of the request header v2
            . '02'                                         // the topic array: the varint 1 + 1
            . '0b' . bin2hex('t2-24-flex')                 // the name: the varint 10 + 1
            . '00'                                         // the tag buffer of the topic structure
            . '00' . '00' . '00'                           // the three booleans of the versions 4 and 8
            . '00',                                        // the tag buffer of the body
            $flexible
        );
        self::assertTrue(MetadataRequestV9::isFlexible());
        self::assertTrue(MetadataRequest::isFlexible());
        self::assertSame(9, MetadataRequest::FLEXIBLE_VERSION);
        self::assertFalse(MetadataRequestV8::isFlexible());

        // The same question at version 8 needs four bytes for the array and two for the length of the name, and
        // has no tagged-field section anywhere
        $plain = bin2hex((string) new MetadataRequestV8(['t2-24-flex'], false, 't2', 600));

        self::assertSame(
            '0000001f'
            . '0003' . '0008' . '00000258'
            . '0002' . '7432'
            . '00000001' . '000a' . bin2hex('t2-24-flex')
            . '00' . '00' . '00',
            $plain
        );
        self::assertSame(['t2-24-flex'], new MetadataRequest(['t2-24-flex'])->getTopics());
        self::assertSame(['t2-24-flex'], new MetadataRequestV8(['t2-24-flex'])->getTopics());
    }

    public function testRequestTopicsAreNotNullableInVersionZero(): void
    {
        // The nullable topic array only arrived with version 1 of this API (Kafka 0.10.0)
        self::assertSame([BinarySchema::TYPE_STRING], MetadataRequestV0::getScheme()['topics']);
        self::assertSame(
            [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true],
            MetadataRequestV8::getScheme()['topics']
        );

        // From version 9 the entry is a structure of the specification, because a flexible version closes every
        // structure with a tagged-field section - a bare compact string would be one byte short (KIP-482)
        self::assertSame(
            [MetadataRequestTopic::class, BinarySchema::FLAG_NULLABLE => true],
            MetadataRequest::getScheme()['topics']
        );
        self::assertSame(['orders'], new MetadataRequest(['orders'])->getTopics(), 'the public shape is the name');
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
        $response = MetadataResponseV2::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)));

        self::assertSame(
            ['messageSize', 'correlationId', 'brokers', 'clusterId', 'controllerId', 'topics'],
            array_keys(MetadataResponseV2::getScheme()),
            'version 2 inserts the cluster id BEFORE the controller id'
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'brokers', 'clusterId', 'controllerId', 'topics'],
            array_keys(MetadataResponseV7::getScheme()),
            'version 3 puts the throttle time in front of everything, and version 4 answers the same frame'
        );
        self::assertSame(
            ['messageSize', 'correlationId', 'throttleTimeMs', 'brokers', 'clusterId', 'controllerId', 'topics',
                'clusterAuthorizedOperations'],
            array_keys(MetadataResponseV8::getScheme()),
            'version 8 (KIP-430) appends the cluster-wide bitfield to the end of the frame'
        );
        self::assertSame(
            array_keys(MetadataResponseV7::getScheme()),
            array_keys(MetadataResponseV3::getScheme()),
            'METADATA_RESPONSE_V4 = METADATA_RESPONSE_V3'
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

        $response = MetadataResponseV2::unpack(new StringStream($frame));

        self::assertTrue($response->topics['__consumer_offsets']->isInternal);
    }

    public function testAVersionThreeAnswerStartsWithTheThrottleTime(): void
    {
        //   ThrottleTimeMs 0, [Broker] => none, ClusterId null, ControllerId 0, no topics
        $frame = '00000016' . '0000002a' . '00000000' . '00000000' . 'ffff' . '00000000' . '00000000';

        $response = MetadataResponseV3::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame([], $response->brokers);
        self::assertNull($response->clusterId);
        self::assertSame(0, $response->controllerId);
        self::assertSame($frame, bin2hex((string) $response));
    }

    public function testAVersionFourAnswerIsReadWithTheVersionThreeLayout(): void
    {
        $frame = '00000016' . '0000002a' . '00000000' . '00000000' . 'ffff' . '00000000' . '00000000';

        $versionThree = MetadataResponseV3::unpack(new StringStream((string) hex2bin($frame)));
        $versionFour  = MetadataResponseV4::unpack(new StringStream((string) hex2bin($frame)));

        self::assertSame(bin2hex((string) $versionThree), bin2hex((string) $versionFour));
        self::assertSame($versionThree->throttleTimeMs, $versionFour->throttleTimeMs);
        self::assertSame(MetadataResponseV3::getScheme(), MetadataResponseV4::getScheme());
    }

    public function testAVersionFiveAnswerCarriesTheOfflineReplicasOfEveryPartition(): void
    {
        //   ThrottleTimeMs 0, one broker 0 at "kafka-1:9092" without a rack, ClusterId null, ControllerId 0, the
        //   topic "orders" with one partition: leader 0, Replicas [0, 1], Isr [0] and OfflineReplicas [1] - the
        //   replica on the broker 1, which is down or whose log directory failed (KIP-112/113)
        $frame = self::OFFLINE_REPLICAS_RESPONSE_V5_HEX;

        $response  = MetadataResponseV5::unpack(new StringStream((string) hex2bin($frame)));
        $partition = $response->topics['orders']->partitions[0];

        self::assertSame([0, 1], $partition->replicas);
        self::assertSame([0], $partition->isr);
        self::assertSame([1], $partition->offlineReplicas);
        self::assertSame($frame, bin2hex((string) $response), 'the answer has to survive a round trip');
    }

    public function testAVersionBelowFiveLeavesTheOfflineReplicasEmpty(): void
    {
        //   The very same answer without the four plus four bytes of the OfflineReplicas array
        $frame = '00000056' . '0000002a' . '00000000'
            . '00000001' . '00000000' . '0007' . '6b61666b612d31' . '00002384' . 'ffff'
            . 'ffff' . '00000000'
            . '00000001' . '0000' . '0006' . '6f7264657273' . '00'
            . '00000001' . '0000' . '00000000' . '00000000'
            . '00000002' . '00000000' . '00000001'
            . '00000001' . '00000000';

        $response  = MetadataResponseV4::unpack(new StringStream((string) hex2bin($frame)));
        $partition = $response->topics['orders']->partitions[0];

        self::assertSame([0, 1], $partition->replicas);
        self::assertSame([0], $partition->isr);
        self::assertSame(
            [],
            $partition->offlineReplicas,
            'an answer below version 5 does not say anything about offline replicas'
        );
        self::assertSame($frame, bin2hex((string) $response), 'the answer has to survive a round trip');
    }

    public function testEveryVersionOfTheAnswerReadsTheEntriesOfItsOwnVersion(): void
    {
        self::assertArrayHasKey('offlineReplicas', PartitionMetadata::getScheme());
        self::assertArrayNotHasKey('offlineReplicas', PartitionMetadataV0::getScheme());
        self::assertSame(
            ['topic' => TopicMetadata::class],
            MetadataResponse::getScheme()['topics'],
            'version 5 reads the partition entries with the offline replicas'
        );
        self::assertSame(
            ['topic' => TopicMetadataV8::class],
            MetadataResponseV9::getScheme()['topics'],
            'and the versions 8 and 9 the topic entries without a topic id'
        );
        self::assertSame(['topic' => TopicMetadataV1::class], MetadataResponseV4::getScheme()['topics']);
        self::assertSame(['topic' => TopicMetadataV1::class], MetadataResponseV3::getScheme()['topics']);
        self::assertSame(['topic' => TopicMetadataV1::class], MetadataResponseV1::getScheme()['topics']);
        self::assertSame(['topic' => TopicMetadataV0::class], MetadataResponseV0::getScheme()['topics']);
    }

    public function testControllerIdIsMinusOneWhileTheClusterElectsAController(): void
    {
        //   [Broker] => none, ClusterId null, ControllerId -1, no topics
        $frame = hex2bin('00000012' . '0000002a' . '00000000' . 'ffff' . 'ffffffff' . '00000000');

        $response = MetadataResponseV2::unpack(new StringStream($frame));

        self::assertNull($response->clusterId, 'a broker without a cluster id answers a null string');
        self::assertSame(MetadataResponse::NO_CONTROLLER_ID, $response->controllerId);
        self::assertSame(-1, MetadataResponse::NO_CONTROLLER_ID);
    }

    public function testResponseKeepsTheReplicaAndIsrSetsOfEveryPartition(): void
    {
        $partitions = MetadataResponseV2::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)))
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
        $response = MetadataResponseV2::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)));

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
        $response = MetadataResponseV2::unpack(new StringStream(hex2bin(self::CLUSTER_RESPONSE_V2_HEX)));

        /** @var MetadataResponseV2 $restored */
        $restored = eval('return ' . var_export($response, true) . ';');

        self::assertInstanceOf(MetadataResponseV2::class, $restored);
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
