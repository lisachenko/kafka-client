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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\QuorumInfo;
use Protocol\Kafka\Admin\ReplicaState;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaState;
use Protocol\Kafka\Protocol\Request\DescribeQuorumRequest;
use Protocol\Kafka\Protocol\Request\DescribeQuorumRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeQuorumResponse;
use Protocol\Kafka\Protocol\Request\DescribeQuorumResponseV0;

/**
 * Exercises DescribeQuorum (key 55, v0 and v1) against the 3.9.2 KRaft node.
 *
 * The api reads the state of the **raft quorum** that KIP-595 put in the place of ZooKeeper, and the node of this
 * line is the smallest quorum there is: one combined node, which is its own leader, its own single voter and no
 * observer at all. The version 1 of KIP-836 adds the two timestamps of a replica state, which is the whole
 * difference between the two answers this class asks for.
 *
 * It creates nothing on the broker and therefore has nothing to clean up: the metadata quorum is read-only from
 * the outside, and the topic it asks about on purpose is one that does not exist.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeQuorumRequest::class)]
#[CoversClass(DescribeQuorumRequestV0::class)]
#[CoversClass(DescribeQuorumResponse::class)]
#[CoversClass(DescribeQuorumResponseV0::class)]
#[CoversClass(DescribeQuorumResponseReplicaState::class)]
#[CoversClass(QuorumInfo::class)]
#[CoversClass(ReplicaState::class)]
final class MetadataQuorumApiTest extends IntegrationTestCase
{
    /**
     * A topic name no suite of this repository creates, so that the "not a quorum" answer is reproducible
     */
    private const string ABSENT_TOPIC = 't1-33-not-a-quorum';

    private const string CLIENT_ID = 'kafka-client-t1-33';

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * The quorum of the node: one voter, which is the leader itself, and no observer
     */
    public function testTheNodeIsItsOwnQuorum(): void
    {
        $quorum = $this->admin->describeMetadataQuorum();

        self::assertSame(1, $quorum->leaderId, 'the node id of the image, which is 1 and never 0');
        self::assertGreaterThan(0, $quorum->leaderEpoch);
        self::assertGreaterThan(0, $quorum->highWatermark, 'the offset the metadata log is replicated to');
        self::assertCount(1, $quorum->voters, 'a combined node is the whole quorum');
        self::assertSame([], $quorum->observers, 'and a voter is never an observer of itself');

        $leader = $quorum->voters[0];

        self::assertSame(1, $leader->replicaId);
        self::assertSame($leader, $quorum->replicaState(1));
        self::assertNull($quorum->replicaState(4242), 'a node that is not in the quorum has no state');
        self::assertGreaterThanOrEqual(
            $quorum->highWatermark,
            $leader->logEndOffset,
            'the leader has written at least as much as the quorum has replicated'
        );
    }

    /**
     * The two timestamps of KIP-836 are the current time of the leader for its own entry, not the -1 of the spec
     *
     * `DescribeQuorumResponse.json` @ 3.3.2 says `LastFetchTimestamp` "is reported as -1 both for the current
     * leader or if it is unknown for a voter", but `LeaderState.describeReplicaState` @ 3.3.2 sets **both**
     * timestamps to `currentTimeMs` when the entry is the leader's own, and that is what the node answers: a real
     * millisecond that moves between two requests.
     */
    public function testTheLeaderReportsItsOwnTimestampsAsTheCurrentTime(): void
    {
        $before = (int) (microtime(true) * 1000);
        $first  = $this->admin->describeMetadataQuorum()->voters[0];
        usleep(20000);
        $second = $this->admin->describeMetadataQuorum()->voters[0];
        $after  = (int) (microtime(true) * 1000);

        self::assertNotNull($first->lastFetchTimestamp, 'not the -1 the `about` of the field announces');
        self::assertNotNull($first->lastCaughtUpTimestamp);
        self::assertSame(
            $first->lastFetchTimestamp,
            $first->lastCaughtUpTimestamp,
            'the leader answers the same `currentTimeMs` in both of its own fields'
        );
        self::assertGreaterThanOrEqual($before - 60000, $first->lastFetchTimestamp, 'the clock of the container');
        self::assertLessThanOrEqual($after + 60000, $first->lastFetchTimestamp);
        self::assertGreaterThan(
            $first->lastFetchTimestamp,
            (int) $second->lastFetchTimestamp,
            'and it is read anew for every request'
        );
    }

    /**
     * A version 0 answer has no timestamp at all, and the value object reports both as null
     */
    public function testTheVersionZeroAnswerHasNoTimestamps(): void
    {
        $stream = $this->connect($this->configuration());

        DescribeQuorumRequestV0::metadataQuorum(self::CLIENT_ID, 3320)->writeTo($stream);
        $v0 = DescribeQuorumResponseV0::unpack($stream);

        DescribeQuorumRequest::metadataQuorum(self::CLIENT_ID, 3321)->writeTo($stream);
        $v1 = DescribeQuorumResponse::unpack($stream);

        $topic = DescribeQuorumRequest::CLUSTER_METADATA_TOPIC;

        $voterV0 = $v0->topics[$topic]->partitions[0]->currentVoters[1];
        $voterV1 = $v1->topics[$topic]->partitions[0]->currentVoters[1];

        self::assertSame(KafkaException::NO_ERROR, $v0->errorCode);
        self::assertSame(1, $v0->topics[$topic]->partitions[0]->leaderId);
        self::assertSame(
            DescribeQuorumResponseReplicaState::UNKNOWN_TIMESTAMP,
            $voterV0->lastFetchTimestamp,
            'the version 0 has no field for it, so the default of the class stands'
        );
        self::assertSame(
            DescribeQuorumResponseReplicaState::UNKNOWN_TIMESTAMP,
            $voterV0->lastCaughtUpTimestamp
        );
        self::assertNotSame(
            DescribeQuorumResponseReplicaState::UNKNOWN_TIMESTAMP,
            $voterV1->lastFetchTimestamp,
            'while the version 1 carries the real one'
        );
        self::assertSame(
            16,
            strlen((string) $v1) - strlen((string) $v0),
            'the two timestamps of the one voter, and nothing else'
        );

        $stream->disconnect();
    }

    /**
     * A topic that is not replicated by a raft quorum is 3 per partition, and the top level stays 0
     */
    public function testATopicThatIsNotTheMetadataLogIsUnknownPerPartition(): void
    {
        $stream = $this->connect($this->configuration());

        new DescribeQuorumRequest([self::ABSENT_TOPIC => [0]], self::CLIENT_ID, 3322)->writeTo($stream);
        $answer = DescribeQuorumResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'the top level does not carry it');

        $partition = $answer->topics[self::ABSENT_TOPIC]->partitions[0];

        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $partition->errorCode);
        self::assertSame(0, $partition->leaderId, 'the zero of the type, not the -1 of "the leader is unknown"');
        self::assertSame(0, $partition->leaderEpoch);
        self::assertSame(0, $partition->highWatermark);
        self::assertSame([], $partition->currentVoters);
        self::assertSame([], $partition->observers);

        $stream->disconnect();
    }

    /**
     * A request that names no topic is not refused: the answer is the code 0 and nine bytes
     */
    public function testAnEmptyTopicArrayIsAnsweredWithNothing(): void
    {
        $stream = $this->connect($this->configuration());

        new DescribeQuorumRequest([], self::CLIENT_ID, 3323)->writeTo($stream);
        $answer = DescribeQuorumResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame([], $answer->topics);
        self::assertSame(
            '0000000900000cfb0000000100',
            bin2hex((string) $answer),
            'the size, the correlation id, the two tag buffers, the code 0 and an empty compact array'
        );

        $stream->disconnect();
    }

    /**
     * The decoded answer of the node is byte-exactly what the vector file replays
     */
    public function testTheAnswerOfTheNodeRoundTripsThroughTheSchema(): void
    {
        $stream = $this->connect($this->configuration());

        new DescribeQuorumRequest([DescribeQuorumRequest::CLUSTER_METADATA_TOPIC => [0]], self::CLIENT_ID, 3324)
            ->writeTo($stream);
        $answer = DescribeQuorumResponse::unpack($stream);
        $frame  = (string) $answer;

        self::assertSame(
            bin2hex($frame),
            bin2hex((string) DescribeQuorumResponse::unpack(new StringStream($frame))),
            're-encoding what the node sent reproduces it'
        );

        $stream->disconnect();
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
