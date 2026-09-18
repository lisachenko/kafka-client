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
use Protocol\Kafka\Admin\QuorumInfo;
use Protocol\Kafka\Admin\ReplicaState;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeQuorumRequestPartition;
use Protocol\Kafka\Protocol\Data\DescribeQuorumRequestTopic;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaState;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaStateV0;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopic;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopicV0;
use Protocol\Kafka\Protocol\Request\DescribeQuorumRequest;
use Protocol\Kafka\Protocol\Request\DescribeQuorumRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeQuorumResponse;
use Protocol\Kafka\Protocol\Request\DescribeQuorumResponseV0;

/**
 * Byte-exact tests for DescribeQuorum (key 55, v0 and v1), the raft api a `broker` listener serves.
 *
 * The api is flexible from its version 0, so every string and every array of both frames is compact and every
 * structure ends in a tagged-field section. The **request** of the two versions is the same frame - "Version 1
 * adds additional fields in the response. The request is unchanged (KIP-836)" - and what version 1 adds are the
 * two timestamps of every replica state.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
#[CoversClass(DescribeQuorumRequest::class)]
#[CoversClass(DescribeQuorumRequestV0::class)]
#[CoversClass(DescribeQuorumResponse::class)]
#[CoversClass(DescribeQuorumResponseV0::class)]
#[CoversClass(DescribeQuorumRequestTopic::class)]
#[CoversClass(DescribeQuorumRequestPartition::class)]
#[CoversClass(DescribeQuorumResponseTopic::class)]
#[CoversClass(DescribeQuorumResponseTopicV0::class)]
#[CoversClass(DescribeQuorumResponsePartition::class)]
#[CoversClass(DescribeQuorumResponsePartitionV0::class)]
#[CoversClass(DescribeQuorumResponseReplicaState::class)]
#[CoversClass(DescribeQuorumResponseReplicaStateV0::class)]
#[CoversClass(QuorumInfo::class)]
#[CoversClass(ReplicaState::class)]
final class DescribeQuorumTest extends TestCase
{
    /**
     * The request for the metadata quorum, the one every `describeMetadataQuorum()` sends.
     *
     *   Size          => 00 00 00 2b (43 bytes)
     *   ApiKey        => 00 37 (55), ApiVersion => 00 01
     *   CorrelationId => 00 00 00 09
     *   ClientId      => 00 04 "test", TAG_BUFFER => 00
     *   Topics        => 02 (1 + 1)
     *     TopicName   => 13 "__cluster_metadata" (18 + 1)
     *     Partitions  => 02 (1 + 1) -> PartitionIndex 00 00 00 00, TAG_BUFFER 00
     *     TAG_BUFFER  => 00
     *   TAG_BUFFER    => 00
     */
    private const string REQUEST_HEX = '0000002b'
        . '0037'
        . '0001'
        . '00000009'
        . '0004' . '74657374'
        . '00'
        . '02'
        . '13' . '5f5f636c75737465725f6d65746164617461'
        . '02' . '00000000' . '00'
        . '00'
        . '00';

    /**
     * An answer of a three-node quorum: two voters and one observer, with the timestamps of KIP-836.
     *
     *   Size          => 00 00 00 8e (142 bytes)
     *   CorrelationId => 00 00 00 09, TAG_BUFFER => 00
     *   ErrorCode     => 00 00
     *   Topics        => 02 -> 13 "__cluster_metadata", Partitions 02 ->
     *     PartitionIndex 00 00 00 00, ErrorCode 00 00, LeaderId 00 00 00 01, LeaderEpoch 00 00 00 04,
     *     HighWatermark 00 00 00 00 00 00 10 00,
     *     CurrentVoters 03 -> [1] 1000/-1/-1 (the leader reports its own -1 in this frame),
     *                         [2] 0999/1640995200000/1640995100000,
     *     Observers     02 -> [3] 0998/1640995199000/1640995099000
     */
    private const string RESPONSE_HEX = '0000008e'
        . '00000009'
        . '00'
        . '0000'
        . '02'
        . '13' . '5f5f636c75737465725f6d65746164617461'
        . '02'
        . '00000000'
        . '0000'
        . '00000001'
        . '00000004'
        . '0000000000001000'
        . '03'
        . '00000001' . '0000000000001000' . 'ffffffffffffffff' . 'ffffffffffffffff' . '00'
        . '00000002' . '0000000000000fff' . '0000017e12ef9c00' . '0000017e12ee1560' . '00'
        . '02'
        . '00000003' . '0000000000000ffe' . '0000017e12ef9818' . '0000017e12ee1178' . '00'
        . '00'
        . '00'
        . '00';

    public function testTheRequestIsPackedAccordingToTheSpec(): void
    {
        $request = DescribeQuorumRequest::metadataQuorum('test', 9);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_QUORUM, $request->getApiKey());
        self::assertSame(1, $request->getApiVersion());
        self::assertTrue(DescribeQuorumRequest::isFlexible(), 'the api is flexible from its version 0');
        self::assertSame(DescribeQuorumRequest::HEADER_V2, $request->getHeaderVersion());
        self::assertSame(
            ['__cluster_metadata'],
            array_keys($request->getTopics()),
            'the one topic a KRaft cluster replicates with raft'
        );
        self::assertSame([0], array_keys($request->getTopics()['__cluster_metadata']->partitions));
    }

    /**
     * The version 0 request is the version 1 request with another byte in the header, and nothing else
     */
    public function testTheVersionZeroRequestIsTheSameFrame(): void
    {
        $v0 = DescribeQuorumRequestV0::metadataQuorum('test', 9);

        self::assertSame(0, $v0->getApiVersion());
        self::assertSame(
            str_replace('00370001', '00370000', self::REQUEST_HEX),
            bin2hex((string) $v0),
            'KIP-836 added fields to the answer alone'
        );
    }

    public function testTheAnswerCarriesTheVotersAndTheObserversOfTheQuorum(): void
    {
        $response = DescribeQuorumResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'there is no throttle time before it');
        self::assertSame(['__cluster_metadata'], array_keys($response->topics));

        $partition = $response->topics['__cluster_metadata']->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(1, $partition->leaderId);
        self::assertSame(4, $partition->leaderEpoch);
        self::assertSame(4096, $partition->highWatermark);
        self::assertSame([1, 2], array_keys($partition->currentVoters), 'indexed by the replica id');
        self::assertSame([3], array_keys($partition->observers));

        $follower = $partition->currentVoters[2];

        self::assertSame(4095, $follower->logEndOffset);
        self::assertSame(1640995200000, $follower->lastFetchTimestamp);
        self::assertSame(1640995100000, $follower->lastCaughtUpTimestamp);
        self::assertSame(
            DescribeQuorumResponseReplicaState::UNKNOWN_TIMESTAMP,
            $partition->currentVoters[1]->lastFetchTimestamp,
            'the -1 of a replica the leader knows no timestamp of'
        );
        self::assertSame(self::RESPONSE_HEX, bin2hex((string) $response), 'and the frame survives the round trip');
    }

    /**
     * A version 0 answer has no timestamps at all, and the two fields keep the -1 of the specification
     */
    public function testTheVersionZeroAnswerStopsAtTheLogEndOffset(): void
    {
        $hex = '00000044'
            . '00000009'
            . '00'
            . '0000'
            . '02'
            . '13' . '5f5f636c75737465725f6d65746164617461'
            . '02'
            . '00000000'
            . '0000'
            . '00000001'
            . '00000004'
            . '0000000000001000'
            . '02' . '00000001' . '0000000000001000' . '00'
            . '01'
            . '00'
            . '00'
            . '00';

        $response = DescribeQuorumResponseV0::unpack(new StringStream((string) hex2bin($hex)));
        $voter    = $response->topics['__cluster_metadata']->partitions[0]->currentVoters[1];

        self::assertInstanceOf(DescribeQuorumResponseReplicaStateV0::class, $voter);
        self::assertSame(4096, $voter->logEndOffset);
        self::assertSame(DescribeQuorumResponseReplicaState::UNKNOWN_TIMESTAMP, $voter->lastFetchTimestamp);
        self::assertSame(DescribeQuorumResponseReplicaState::UNKNOWN_TIMESTAMP, $voter->lastCaughtUpTimestamp);
        self::assertSame([], $response->topics['__cluster_metadata']->partitions[0]->observers);
        self::assertSame($hex, bin2hex((string) $response));
    }

    /**
     * A top-level error is the whole answer: there is no topic behind it, and no message either
     */
    public function testATopLevelErrorAnswersNoTopicAtAll(): void
    {
        $hex = '00000009' . '00000009' . '00' . '001f' . '01' . '00';

        $response = DescribeQuorumResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $response->errorCode);
        self::assertSame([], $response->topics);
        self::assertSame($hex, bin2hex((string) $response));
    }

    /**
     * The value object of the admin api reads a -1 as "the leader does not know", as the Java client does
     */
    public function testTheQuorumInfoFindsAReplicaWhereverItStands(): void
    {
        $leader   = new ReplicaState(1, 4096);
        $follower = new ReplicaState(2, 4095, 1640995200000, 1640995100000);
        $observer = new ReplicaState(3, 4094, 1640995199000, 1640995099000);
        $quorum   = new QuorumInfo(1, 4, 4096, [$leader, $follower], [$observer]);

        self::assertSame($leader, $quorum->replicaState(1));
        self::assertSame($observer, $quorum->replicaState(3), 'an observer is found as well as a voter');
        self::assertNull($quorum->replicaState(42), 'and a node that is not in the quorum is null');
        self::assertNull($leader->lastFetchTimestamp, 'an unknown timestamp is null, not -1');
        self::assertSame(4, $quorum->leaderEpoch);
    }
}
