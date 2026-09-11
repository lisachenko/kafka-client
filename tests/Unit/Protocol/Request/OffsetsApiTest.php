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
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartitionV0;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartitionV1;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartitionV1;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopicV0;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV2;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV3;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV1;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV2;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV3;

/**
 * Byte-exact tests for the Offsets (ListOffset) API, versions 0, 1 and 2.
 *
 * <pre>
 *   OffsetRequest v0  => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
 *   OffsetResponse v0 => [TopicName [Partition ErrorCode [Offset]]]
 *   ListOffsets Request  (Version: 1) => replica_id [topic [partition timestamp]]
 *   ListOffsets Response (Version: 1) => [topic [partition error_code timestamp offset]]
 *   ListOffsets Request  (Version: 2) => replica_id isolation_level [topic [partition timestamp]]
 *   ListOffsets Response (Version: 2) => throttle_time_ms [topic [partition error_code timestamp offset]]
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v4), a.k.a. ListOffset"
 */
#[CoversClass(OffsetsRequest::class)]
#[CoversClass(OffsetsRequestV0::class)]
#[CoversClass(OffsetsRequestV1::class)]
#[CoversClass(OffsetsResponse::class)]
#[CoversClass(OffsetsResponseV0::class)]
#[CoversClass(OffsetsResponseV1::class)]
#[CoversClass(OffsetsRequestTopic::class)]
#[CoversClass(OffsetsRequestTopicV0::class)]
#[CoversClass(OffsetsRequestPartition::class)]
#[CoversClass(OffsetsRequestPartitionV0::class)]
#[CoversClass(OffsetsResponseTopic::class)]
#[CoversClass(OffsetsResponseTopicV0::class)]
#[CoversClass(OffsetsResponsePartition::class)]
#[CoversClass(OffsetsResponsePartitionV0::class)]
final class OffsetsApiTest extends TestCase
{
    /**
     * Offsets request v2 asking for the latest offset of "topic-0", client id "test", correlation id 7.
     *
     *   Size            => 00 00 00 2e (46 bytes)
     *   ApiKey          => 00 02
     *   ApiVersion      => 00 04
     *   CorrelationId   => 00 00 00 07
     *   ClientId        => 00 04 "test"
     *   ReplicaId       => ff ff ff ff (-1, an ordinary consumer)
     *   IsolationLevel  => 00 (read_uncommitted)
     *   [TopicName]     => 00 00 00 01, 00 05 "topic"
     *     [Partition]   => 00 00 00 01
     *       Partition   => 00 00 00 00
     *       CurrentLeaderEpoch => ff ff ff ff (-1, "I do not know the epoch"), since version 4
     *       Timestamp   => ff ff ff ff ff ff ff ff (-1, the latest offset)
     */
    private const string LATEST_REQUEST_HEX = '00000032'
        . '0002'
        . '0004'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'ffffffff' . 'ffffffffffffffff';

    /**
     * The same frame with the api version 3, whose partition entries carry no leader epoch
     */
    private const string LATEST_REQUEST_V3_HEX = '0000002e'
        . '0002'
        . '0003'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'ffffffffffffffff';

    /**
     * The same request with the isolation level `read_committed`, which asks for the last stable offset
     */
    private const string LATEST_COMMITTED_REQUEST_HEX = '00000032'
        . '0002'
        . '0004'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '01'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'ffffffff' . 'ffffffffffffffff';

    /**
     * The same question as a version 1 frame, which has no isolation level at all
     */
    private const string LATEST_REQUEST_V1_HEX = '0000002d'
        . '0002'
        . '0001'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'ffffffffffffffff';

    /**
     * The same request asking for the earliest available offset: the timestamp is -2 instead of -1
     */
    private const string EARLIEST_REQUEST_HEX = '00000032'
        . '0002'
        . '0004'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'ffffffff' . 'fffffffffffffffe';

    /**
     * The version 0 request of the same question, which carries the api version 0 and a MaxNumberOfOffsets of 1
     */
    private const string LATEST_REQUEST_V0_HEX = '00000031'
        . '0002'
        . '0000'
        . '00000007'
        . '0004' . '74657374'
        . 'ffffffff'
        . '00000001'
        . '0005' . '746f706963'
        . '00000001'
        . '00000000' . 'ffffffffffffffff' . '00000001';

    public function testLatestOffsetRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetsRequest(
            ['topic' => [0 => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7
        );

        self::assertSame(self::LATEST_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(50, $request->getMessageSize(), 'the four epoch bytes of KIP-320 per partition');
        self::assertSame(4, $request->getApiVersion());
    }

    public function testTheVersionsTwoAndThreeSendOneAndTheSameFrame(): void
    {
        // `ListOffsetsRequest.json` @ 2.8.2 says "Version 3 is the same as version 2": what version 3 (Kafka 2.0,
        // KIP-219) states is that the client waits out `throttle_time_ms` itself, because the broker answers a
        // throttled request first and mutes the channel afterwards - only the api version of the header says so
        $version2 = bin2hex((string) new OffsetsRequestV2(
            ['topic' => [0 => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7
        ));
        $version3 = bin2hex((string) new OffsetsRequestV3(
            ['topic' => [0 => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7
        ));

        self::assertSame(substr_replace(self::LATEST_REQUEST_V3_HEX, '0002', 12, 4), $version2);
        self::assertSame(self::LATEST_REQUEST_V3_HEX, $version3);
        self::assertSame(OffsetsRequestV2::getScheme(), OffsetsRequestV3::getScheme());
        self::assertSame(OffsetsResponseV2::getScheme(), OffsetsResponseV3::getScheme());
        self::assertSame(2, OffsetsRequestV2::VERSION);
        self::assertSame(2, OffsetsResponseV2::VERSION);
    }

    public function testVersionFourCarriesTheLeaderEpochOnBothSides(): void
    {
        // KIP-320: `current_leader_epoch` sits between the partition index and the target timestamp of a request,
        // `leader_epoch` behind the offset of an answer - the field order of the JSON, which is the wire order
        $request = new OffsetsRequest(
            ['topic' => [0 => [OffsetsRequest::LATEST, 7]]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7
        );

        self::assertSame(
            substr_replace(self::LATEST_REQUEST_HEX, '00000007', 2 * 42, 8),
            bin2hex((string) $request),
            'a value of the map may be the pair [timestamp, currentLeaderEpoch]'
        );
        self::assertSame(
            ['partition', 'currentLeaderEpoch', 'timestamp'],
            array_keys(OffsetsRequestPartition::getScheme())
        );
        self::assertSame(
            ['partition', 'timestamp'],
            array_keys(OffsetsRequestPartitionV1::getScheme()),
            'the versions 1, 2 and 3 carry no epoch at all'
        );
        self::assertSame(
            ['partition', 'errorCode', 'timestamp', 'offset', 'leaderEpoch'],
            array_keys(OffsetsResponsePartition::getScheme())
        );
        self::assertSame(
            ['partition', 'errorCode', 'timestamp', 'offset'],
            array_keys(OffsetsResponsePartitionV1::getScheme())
        );
        self::assertSame(-1, OffsetsRequestPartition::UNKNOWN_LEADER_EPOCH);
        self::assertSame(-1, OffsetsResponsePartition::UNKNOWN_LEADER_EPOCH);
        self::assertSame(4, OffsetsRequest::VERSION);
        self::assertSame(4, OffsetsResponse::VERSION);
    }

    public function testEarliestOffsetRequestIsPackedAccordingToTheSpec(): void
    {
        $request = new OffsetsRequest(
            ['topic' => [0 => OffsetsRequest::EARLIEST]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7
        );

        self::assertSame(self::EARLIEST_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testVersionZeroRequestKeepsTheMaxNumberOfOffsetsField(): void
    {
        $request = new OffsetsRequestV0(['topic' => [0 => OffsetsRequest::LATEST]], 1, -1, 'test', 7);

        self::assertSame(self::LATEST_REQUEST_V0_HEX, bin2hex((string) $request));
        self::assertSame(0, $request->getApiVersion());
    }

    public function testSpecialTimeValuesFollowTheSpec(): void
    {
        self::assertSame(-1, OffsetsRequest::LATEST);
        self::assertSame(-2, OffsetsRequest::EARLIEST);
        self::assertSame(-1, OffsetsRequest::CONSUMER_REPLICA_ID);
        self::assertSame(-2, OffsetsRequest::DEBUGGING_REPLICA_ID);
        self::assertSame(-1, OffsetsResponsePartition::UNKNOWN_TIMESTAMP);
        self::assertSame(-1, OffsetsResponsePartition::UNKNOWN_OFFSET);
    }

    public function testRequestAcceptsStructuredTopicPartitions(): void
    {
        $request = OffsetsRequest::fromTopicPartitions(
            [new TopicPartition('topic', 0)],
            OffsetsRequest::EARLIEST,
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7
        );

        self::assertSame(self::EARLIEST_REQUEST_HEX, bin2hex((string) $request));
    }

    public function testTheIsolationLevelIsTheByteBehindTheReplicaId(): void
    {
        $request = new OffsetsRequest(
            ['topic' => [0 => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_COMMITTED,
            'test',
            7
        );

        self::assertSame(self::LATEST_COMMITTED_REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(0, FetchRequest::READ_UNCOMMITTED);
        self::assertSame(1, FetchRequest::READ_COMMITTED);
    }

    public function testTheVersionOneRequestCarriesNoIsolationLevel(): void
    {
        $request = new OffsetsRequestV1(
            ['topic' => [0 => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_COMMITTED,
            'test',
            7
        );

        self::assertSame(
            self::LATEST_REQUEST_V1_HEX,
            bin2hex((string) $request),
            'the field arrived with version 2, so a version 1 frame is served as read_uncommitted'
        );
        self::assertSame(1, $request->getApiVersion());
        self::assertArrayNotHasKey('isolationLevel', OffsetsRequestV1::getScheme());
    }

    public function testVersionZeroRequestAcceptsStructuredTopicPartitions(): void
    {
        $request = OffsetsRequestV0::fromTopicPartitions(
            [new TopicPartition('topic', 0)],
            OffsetsRequest::LATEST,
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7,
            1
        );

        self::assertSame(self::LATEST_REQUEST_V0_HEX, bin2hex((string) $request));
    }

    public function testRequestPacksEveryPartitionWithItsOwnTimestamp(): void
    {
        $request = new OffsetsRequest(
            ['topic' => [0 => OffsetsRequest::EARLIEST, 3 => 1451606400000]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            'test',
            7
        );

        self::assertSame(
            '00000042'
            . '0002' . '0004' . '00000007' . '0004' . '74657374'
            . 'ffffffff'
            . '00'
            . '00000001'
            . '0005' . '746f706963'
            . '00000002'
            . '00000000' . 'ffffffff' . 'fffffffffffffffe'
            . '00000003' . 'ffffffff' . '00000151fa7bdc00',
            bin2hex((string) $request)
        );
    }

    public function testRequestSchemeDropsTheMaxNumberOfOffsetsOfVersionZero(): void
    {
        $scheme = OffsetsRequest::getScheme();

        self::assertSame(
            [
                'messageSize',
                'apiKey',
                'apiVersion',
                'correlationId',
                'clientId',
                'replicaId',
                'isolationLevel',
                'topicPartitions',
            ],
            array_keys($scheme)
        );
        self::assertSame(['topic' => OffsetsRequestTopic::class], $scheme['topicPartitions']);
        self::assertSame(
            [
                'partition'          => BinarySchema::TYPE_INT32,
                'currentLeaderEpoch' => BinarySchema::TYPE_INT32,
                'timestamp'          => BinarySchema::TYPE_INT64,
            ],
            OffsetsRequestPartition::getScheme()
        );
        self::assertSame(
            [
                'partition' => BinarySchema::TYPE_INT32,
                'timestamp' => BinarySchema::TYPE_INT64,
            ],
            OffsetsRequestPartitionV1::getScheme(),
            'the versions 1 to 3 ask with the partition index and the target timestamp alone'
        );
        self::assertSame(
            [
                'partition'          => BinarySchema::TYPE_INT32,
                'timestamp'          => BinarySchema::TYPE_INT64,
                'maxNumberOfOffsets' => BinarySchema::TYPE_INT32,
            ],
            OffsetsRequestPartitionV0::getScheme(),
            'version 0 is the only one that asks for more than one offset per partition'
        );
        self::assertSame(
            ['topic' => OffsetsRequestTopicV0::class],
            OffsetsRequestV0::getScheme()['topicPartitions'],
            'a version 0 request packs the version 0 topic entries'
        );
    }

    public function testResponseSchemeReplacesTheOffsetArrayWithATimestampAndAnOffset(): void
    {
        self::assertSame(
            [
                'partition'   => BinarySchema::TYPE_INT32,
                'errorCode'   => BinarySchema::TYPE_INT16,
                'timestamp'   => BinarySchema::TYPE_INT64,
                'offset'      => BinarySchema::TYPE_INT64,
                'leaderEpoch' => BinarySchema::TYPE_INT32,
            ],
            OffsetsResponsePartition::getScheme()
        );
        self::assertSame(
            [
                'partition' => BinarySchema::TYPE_INT32,
                'errorCode' => BinarySchema::TYPE_INT16,
                'timestamp' => BinarySchema::TYPE_INT64,
                'offset'    => BinarySchema::TYPE_INT64,
            ],
            OffsetsResponsePartitionV1::getScheme(),
            'the leader_epoch of KIP-320 arrived with version 4'
        );
        self::assertSame(
            [
                'partition' => BinarySchema::TYPE_INT32,
                'errorCode' => BinarySchema::TYPE_INT16,
                'offsets'   => [BinarySchema::TYPE_INT64],
            ],
            OffsetsResponsePartitionV0::getScheme()
        );
        self::assertSame(['topic' => OffsetsResponseTopicV0::class], OffsetsResponseV0::getScheme()['topics']);
    }

    public function testResponseCarriesOneOffsetAndTheTimestampOfItsMessage(): void
    {
        // Size = 46: CorrelationId + one topic "topic" + one partition with the timestamp 1600000002000, offset 2
        $frame = (string) hex2bin(
            '00000029'
            . '00000007'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '0000'
            . '00000174876e87d0'
            . '0000000000000002'
        );

        $response = OffsetsResponseV1::unpack(new StringStream($frame));

        self::assertSame(7, $response->getCorrelationId());
        self::assertSame(['topic'], array_keys($response->topics));

        $partition = $response->topics['topic']->partitions[0];
        self::assertSame(0, $partition->partition);
        self::assertSame(0, $partition->errorCode);
        self::assertSame(1600000002000, $partition->timestamp, 'the timestamp of the message that was found');
        self::assertSame(2, $partition->offset);
    }

    public function testResponseReportsAnUnmatchedTimestampWithoutAnError(): void
    {
        // A target timestamp above every message of the log: the error code is 0 and both values are -1
        $frame = (string) hex2bin(
            '00000029'
            . '00000007'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000002'
            . '0000'
            . 'ffffffffffffffff'
            . 'ffffffffffffffff'
        );

        $partition = OffsetsResponseV1::unpack(new StringStream($frame))->topics['topic']->partitions[2];

        self::assertSame(2, $partition->partition);
        self::assertSame(0, $partition->errorCode, 'nothing matched, and that is not an error');
        self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $partition->timestamp);
        self::assertSame(OffsetsResponsePartition::UNKNOWN_OFFSET, $partition->offset);
    }

    public function testResponseCarriesThePerPartitionErrorCode(): void
    {
        // Error code 43 is UnsupportedForMessageFormat, the answer of a topic without message timestamps
        $frame = (string) hex2bin(
            '00000029'
            . '00000007'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000002'
            . '002b'
            . 'ffffffffffffffff'
            . 'ffffffffffffffff'
        );

        $partition = OffsetsResponseV1::unpack(new StringStream($frame))->topics['topic']->partitions[2];

        self::assertSame(43, $partition->errorCode);
        self::assertSame(-1, $partition->offset);
    }

    public function testVersionZeroResponseWithThreeSegmentOffsetsIsUnpacked(): void
    {
        // Size = 53: CorrelationId + one topic "topic" + one partition without an error and three int64 offsets
        $frame = (string) hex2bin(
            '00000035'
            . '00000007'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000000'
            . '0000'
            . '00000003'
            . '00000000000003e8' . '00000000000001f4' . '0000000000000000'
        );

        $response = OffsetsResponseV0::unpack(new StringStream($frame));

        self::assertSame(7, $response->getCorrelationId());

        $partition = $response->topics['topic']->partitions[0];
        self::assertInstanceOf(OffsetsResponsePartitionV0::class, $partition);
        self::assertSame(0, $partition->errorCode);
        self::assertSame([1000, 500, 0], $partition->offsets, 'the newest segment offset comes first');
    }

    public function testVersionZeroResponseCarriesThePerPartitionErrorCodeAndNoOffsets(): void
    {
        // Error code 1 is OffsetOutOfRange; the offset array of a failed partition is empty
        $frame = (string) hex2bin(
            '0000001d'
            . '00000007'
            . '00000001'
            . '0005' . '746f706963'
            . '00000001'
            . '00000002'
            . '0001'
            . '00000000'
        );

        $partition = OffsetsResponseV0::unpack(new StringStream($frame))->topics['topic']->partitions[2];

        self::assertSame(2, $partition->partition);
        self::assertSame(1, $partition->errorCode);
        self::assertSame([], $partition->offsets);
    }
}
