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
use Protocol\Kafka\Admin\QuorumNode;
use Protocol\Kafka\Admin\RaftVoterEndpoint;
use Protocol\Kafka\Admin\ReplicaState;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\DescribeQuorumRequestPartition;
use Protocol\Kafka\Protocol\Data\DescribeQuorumRequestTopic;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseListener;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseNode;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponsePartition;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponsePartitionV1;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaState;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaStateV0;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaStateV1;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopic;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopicV0;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseTopicV1;
use Protocol\Kafka\Protocol\Request\DescribeQuorumRequest;
use Protocol\Kafka\Protocol\Request\DescribeQuorumRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeQuorumRequestV1;
use Protocol\Kafka\Protocol\Request\DescribeQuorumResponse;
use Protocol\Kafka\Protocol\Request\DescribeQuorumResponseV0;
use Protocol\Kafka\Protocol\Request\DescribeQuorumResponseV1;

/**
 * Byte-exact tests for DescribeQuorum (key 55, v0 to v2), the raft api a `broker` listener serves.
 *
 * The api is flexible from its version 0, so every string and every array of every frame is compact and every
 * structure ends in a tagged-field section. The **request** of all three versions is the same frame - "Version 1
 * adds additional fields in the response. The request is unchanged (KIP-836)" and "Version 2 adds additional
 * fields in the response. The request is unchanged (KIP-853)" - and what the two bumps add is in the answer: the
 * timestamps of a replica state (v1) and, on top of them, its directory id, the two error messages and the
 * top-level nodes array (v2).
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
#[CoversClass(DescribeQuorumRequest::class)]
#[CoversClass(DescribeQuorumRequestV0::class)]
#[CoversClass(DescribeQuorumRequestV1::class)]
#[CoversClass(DescribeQuorumResponse::class)]
#[CoversClass(DescribeQuorumResponseV0::class)]
#[CoversClass(DescribeQuorumResponseV1::class)]
#[CoversClass(DescribeQuorumRequestTopic::class)]
#[CoversClass(DescribeQuorumRequestPartition::class)]
#[CoversClass(DescribeQuorumResponseTopic::class)]
#[CoversClass(DescribeQuorumResponseTopicV0::class)]
#[CoversClass(DescribeQuorumResponseTopicV1::class)]
#[CoversClass(DescribeQuorumResponsePartition::class)]
#[CoversClass(DescribeQuorumResponsePartitionV0::class)]
#[CoversClass(DescribeQuorumResponsePartitionV1::class)]
#[CoversClass(DescribeQuorumResponseReplicaState::class)]
#[CoversClass(DescribeQuorumResponseReplicaStateV0::class)]
#[CoversClass(DescribeQuorumResponseReplicaStateV1::class)]
#[CoversClass(DescribeQuorumResponseNode::class)]
#[CoversClass(DescribeQuorumResponseListener::class)]
#[CoversClass(QuorumInfo::class)]
#[CoversClass(QuorumNode::class)]
#[CoversClass(RaftVoterEndpoint::class)]
#[CoversClass(ReplicaState::class)]
final class DescribeQuorumTest extends TestCase
{
    /**
     * The request for the metadata quorum, the one every `describeMetadataQuorum()` sends.
     *
     *   Size          => 00 00 00 2b (43 bytes)
     *   ApiKey        => 00 37 (55), ApiVersion => 00 02
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
        . '0002'
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

    /**
     * The same quorum at the **version 2** (KIP-853): the directory ids, the two error messages and the nodes.
     *
     *   Size          => 00 00 01 04 (260 bytes)
     *   CorrelationId => 00 00 00 09, TAG_BUFFER => 00
     *   ErrorCode     => 00 00, ErrorMessage => 00 (the compact null)
     *   Topics        => 02 -> 13 "__cluster_metadata", Partitions 02 ->
     *     PartitionIndex 00 00 00 00, ErrorCode 00 00, ErrorMessage 01 (the empty string),
     *     LeaderId 00 00 00 01, LeaderEpoch 00 00 00 04, HighWatermark 00 00 00 00 00 00 10 00,
     *     CurrentVoters 03 -> [1] dir 0102..0f10, 1000/-1/-1, [2] dir 1122..ff00, 0999/…/…,
     *     Observers     02 -> [3] the zero directory id, 0998/…/…
     *   Nodes         => 03 -> [1] CONTROLLER localhost:9096, [2] CONTROLLER kafka-2.internal:50000
     *   TAG_BUFFER    => 00
     */
    private const string RESPONSE_V2_HEX = '00000104000000090000000002135f5f636c75737465725f6d6574616461746102000000000000010000000100000004000000000000100003000000010102030405060708090a0b0c0d0e0f100000000000001000ffffffffffffffffffffffffffffffff0000000002112233445566778899aabbccddeeff000000000000000fff0000017e12ef9c000000017e12ee1560000200000003000000000000000000000000000000000000000000000ffe0000017e12ef98180000017e12ee11780000000300000001020b434f4e54524f4c4c45520a6c6f63616c686f73742388000000000002020b434f4e54524f4c4c4552116b61666b612d322e696e7465726e616cc350000000';

    public function testTheRequestIsPackedAccordingToTheSpec(): void
    {
        $request = DescribeQuorumRequest::metadataQuorum('test', 9);

        self::assertSame(self::REQUEST_HEX, bin2hex((string) $request));
        self::assertSame(ApiKeys::DESCRIBE_QUORUM, $request->getApiKey());
        self::assertSame(2, $request->getApiVersion());
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
     * The versions 0 and 1 send the version 2 frame with another byte in the header, and nothing else
     */
    public function testTheLowerVersionsSendTheSameFrame(): void
    {
        $v1 = DescribeQuorumRequestV1::metadataQuorum('test', 9);
        $v0 = DescribeQuorumRequestV0::metadataQuorum('test', 9);

        self::assertSame(1, $v1->getApiVersion());
        self::assertSame(0, $v0->getApiVersion());
        self::assertSame(
            str_replace('00370002', '00370001', self::REQUEST_HEX),
            bin2hex((string) $v1),
            'KIP-853 added fields to the answer alone'
        );
        self::assertSame(
            str_replace('00370002', '00370000', self::REQUEST_HEX),
            bin2hex((string) $v0),
            'and so did KIP-836 before it'
        );
    }

    public function testTheAnswerCarriesTheVotersAndTheObserversOfTheQuorum(): void
    {
        $response = DescribeQuorumResponseV1::unpack(new StringStream((string) hex2bin(self::RESPONSE_HEX)));

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
        self::assertNull($response->errorMessage, 'the version 1 has no top-level error message');
        self::assertSame([], $response->nodes, 'and no nodes array either');
        self::assertNull($partition->errorMessage, 'nor an error message per partition');
        self::assertSame(
            Uuid::ZERO,
            $follower->replicaDirectoryId,
            'a version 1 replica state has no directory id, so it stays the zero uuid'
        );
    }

    /**
     * A version 2 answer carries the four additions of KIP-853, and its port is read as an unsigned value
     *
     * The frame is a three-node quorum: two voters with a directory id each, one observer without one, the empty
     * error message the node writes where there is no error, and the top-level `nodes` array with two endpoints -
     * the second of them on the port **50000**, which an int16 would answer as -15536.
     */
    public function testTheVersionTwoAnswerCarriesTheNodesTheDirectoryIdsAndTheErrorMessages(): void
    {
        $response = DescribeQuorumResponse::unpack(new StringStream((string) hex2bin(self::RESPONSE_V2_HEX)));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertNull($response->errorMessage, 'the compact null of a frame that has no message');

        $partition = $response->topics['__cluster_metadata']->partitions[0];

        self::assertInstanceOf(DescribeQuorumResponsePartition::class, $partition);
        self::assertSame('', $partition->errorMessage, 'the empty string of a partition without an error');
        self::assertSame(
            '0102030405060708090a0b0c0d0e0f10',
            bin2hex($partition->currentVoters[1]->replicaDirectoryId)
        );
        self::assertSame(
            Uuid::ZERO,
            $partition->observers[3]->replicaDirectoryId,
            'an observer without a directory id carries the zero uuid'
        );

        self::assertSame([1, 2], array_keys($response->nodes), 'the nodes are indexed by their id');
        self::assertSame(['CONTROLLER'], array_keys($response->nodes[1]->listeners));
        self::assertSame(9096, $response->nodes[1]->listeners['CONTROLLER']->port);
        self::assertSame('localhost', $response->nodes[1]->listeners['CONTROLLER']->host);
        self::assertSame(
            50000,
            $response->nodes[2]->listeners['CONTROLLER']->port,
            'the uint16 of KIP-853: a port above 32767 is not a negative number'
        );
        self::assertSame(self::RESPONSE_V2_HEX, bin2hex((string) $response), 'and the frame round trips');
    }

    /**
     * The value objects of the admin api carry the nodes and the directory ids through
     */
    public function testTheQuorumInfoCarriesTheNodesOfTheVersionTwo(): void
    {
        $endpoint = new RaftVoterEndpoint('CONTROLLER', 'localhost', 9096);
        $node     = new QuorumNode(1, ['CONTROLLER' => $endpoint]);
        $quorum   = new QuorumInfo(1, 4, 4096, [new ReplicaState(1, 4096)], [], [1 => $node]);

        self::assertSame($node, $quorum->node(1));
        self::assertNull($quorum->node(42), 'a node the answer does not name is null');
        self::assertSame($endpoint, $node->endpoint('CONTROLLER'));
        self::assertNull($node->endpoint('PLAINTEXT'), 'and so is a listener it does not advertise');
        self::assertSame('localhost:9096', $endpoint->address());
        self::assertSame(
            'AAAAAAAAAAAAAAAAAAAAAA',
            new ReplicaState(1, 4096)->replicaDirectoryIdAsString(),
            'the zero uuid of a replica without a directory id, as the Kafka tools print it'
        );
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

        $response = DescribeQuorumResponseV1::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $response->errorCode);
        self::assertSame([], $response->topics);
        self::assertSame($hex, bin2hex((string) $response));
    }

    /**
     * The same refusal at the version 2 has a message and an empty nodes array behind it
     */
    public function testATopLevelErrorOfTheVersionTwoCarriesItsMessage(): void
    {
        $message = 'Cluster authorization failed.';
        $hex     = '00000028' . '00000009' . '00' . '001f'
            . '1e' . bin2hex($message)
            . '01'
            . '01'
            . '00';

        $response = DescribeQuorumResponse::unpack(new StringStream((string) hex2bin($hex)));

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $response->errorCode);
        self::assertSame($message, $response->errorMessage);
        self::assertSame([], $response->topics);
        self::assertSame([], $response->nodes);
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
