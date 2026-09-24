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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\DeleteShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\InitializeShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestPartition;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\InitializeShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\InitializeShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryRequest;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryResponse;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Probes the five share-group state apis of KIP-932 (keys 83 to 87, v0) on the 4.3.1 node, one frame each.
 *
 * They are the apis of the **share coordinator**, which a partition leader and a group coordinator send it, and
 * they are **wire only** on this line. This class may therefore change no state at all: the three writing apis
 * (83, 85 and 86) carry a topic without a partition, which the share coordinator answers with an empty result
 * before it looks up a key, and the two reading apis (84 and 87) name the one partition of this class's own topic
 * for a share group that does not exist, with the leader epoch -1, which a read never writes back.
 *
 * @see docs/protocol/4.3.md, section "The share-group state apis (keys 83 to 87) — wire only"
 */
#[CoversClass(InitializeShareGroupStateRequest::class)]
#[CoversClass(InitializeShareGroupStateResponse::class)]
#[CoversClass(ReadShareGroupStateRequest::class)]
#[CoversClass(ReadShareGroupStateResponse::class)]
#[CoversClass(WriteShareGroupStateRequest::class)]
#[CoversClass(WriteShareGroupStateResponse::class)]
#[CoversClass(DeleteShareGroupStateRequest::class)]
#[CoversClass(DeleteShareGroupStateResponse::class)]
#[CoversClass(ReadShareGroupStateSummaryRequest::class)]
#[CoversClass(ReadShareGroupStateSummaryResponse::class)]
final class ShareGroupStateApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-41-share-state';

    /**
     * A share group no suite of this repository creates
     */
    private const string ABSENT_GROUP = 't1-41-no-such-share-group';

    private static ?string $topicId = null;

    protected function setUp(): void
    {
        parent::setUp();

        if (self::$topicId === null) {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID          => self::CLIENT_ID,
                ClientConfig::REQUEST_TIMEOUT_MS => 30000,
            ];
            $topic = self::uniqueTopicName('t1-41-share-state');
            new AdminClient(Cluster::bootstrap($configuration), $configuration)
                ->createTopics([new NewTopic($topic, 1, 1)]);
            // A KRaft node publishes a fresh topic before the broker has applied it, and the share coordinator
            // answers a read of it with the 3 in the meantime (seen once under the load of a full gate): wait until
            // the leader of the partition answers a ListOffsets
            new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
                ->awaitTopicWithLeaders($topic);
            self::$topicId = self::topicIdOf($topic);
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$topicId = null;

        parent::tearDownAfterClass();
    }

    /**
     * A partition that was never initialized cannot be read: the 42 of the share coordinator, and nothing written
     */
    public function testAReadOfAnUninitializedSharePartitionIsInvalidRequest(): void
    {
        $stream = $this->connect();
        new ReadShareGroupStateRequest(self::ABSENT_GROUP, $this->onePartition(), self::CLIENT_ID, 4801)
            ->writeTo($stream);
        $answer = ReadShareGroupStateResponse::unpack($stream);

        self::assertCount(1, $answer->results);
        self::assertSame(self::$topicId, $answer->results[0]->topicId);

        $partition = $answer->results[0]->partitions[0];

        self::assertSame(KafkaException::INVALID_REQUEST, $partition->errorCode);
        self::assertSame('Read operation on uninitialized share partition not allowed.', $partition->errorMessage);
        self::assertSame(0, $partition->stateEpoch);
        self::assertSame(0, $partition->startOffset, 'the default of the field, not the -1 of "not initialized"');
        self::assertSame([], $partition->stateBatches);
    }

    /**
     * The summary of the same partition is no error: the state of a partition the coordinator has never seen
     */
    public function testTheSummaryOfAnUninitializedSharePartitionIsItsInitialState(): void
    {
        $stream = $this->connect();
        new ReadShareGroupStateSummaryRequest(self::ABSENT_GROUP, $this->onePartition(), self::CLIENT_ID, 4802)
            ->writeTo($stream);
        $answer = ReadShareGroupStateSummaryResponse::unpack($stream);

        $partition = $answer->results[0]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertNull($partition->errorMessage);
        self::assertSame(-1, $partition->startOffset, '`PartitionFactory.UNINITIALIZED_START_OFFSET` @ 4.3.1');
        self::assertSame(0, $partition->stateEpoch);
        self::assertSame(0, $partition->leaderEpoch);

        new ReadShareGroupStateSummaryRequest('', $this->onePartition(), self::CLIENT_ID, 4803)->writeTo($stream);

        self::assertSame(
            [],
            ReadShareGroupStateSummaryResponse::unpack($stream)->results,
            'an empty group id is answered with nothing at all'
        );
    }

    /**
     * The three writing apis answer a topic without a partition with an empty result, before any key is looked at
     */
    public function testTheWritingApisAnswerATopicWithoutAPartitionWithNothing(): void
    {
        $stream  = $this->connect();
        $topicId = (string) self::$topicId;

        new InitializeShareGroupStateRequest(
            self::ABSENT_GROUP,
            [new InitializeShareGroupStateRequestTopic($topicId)],
            self::CLIENT_ID,
            4804
        )->writeTo($stream);
        $initialized = InitializeShareGroupStateResponse::unpack($stream);

        new WriteShareGroupStateRequest(
            self::ABSENT_GROUP,
            [new WriteShareGroupStateRequestTopic($topicId)],
            self::CLIENT_ID,
            4805
        )->writeTo($stream);
        $written = WriteShareGroupStateResponse::unpack($stream);

        new DeleteShareGroupStateRequest(
            self::ABSENT_GROUP,
            [new DeleteShareGroupStateRequestTopic($topicId)],
            self::CLIENT_ID,
            4806
        )->writeTo($stream);
        $deleted = DeleteShareGroupStateResponse::unpack($stream);

        foreach ([$initialized, $written, $deleted] as $answer) {
            self::assertSame([], $answer->results);
            self::assertSame(
                7,
                $answer->getMessageSize(),
                'the correlation id, the tag buffer of the header, the empty compact array and the tag buffer'
            );
        }
    }

    /**
     * A principal that may not act as a broker is refused per partition with the 31, and the message says so
     */
    public function testAPrincipalWithoutClusterActionIsRefusedPerPartition(): void
    {
        self::assertNotSame('', self::saslBootstrapServer(), 'the SASL_PLAINTEXT listener of the node');

        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
            ClientConfig::SECURITY_PROTOCOL         => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM            => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME             => 'acltest',
            ClientConfig::SASL_PASSWORD             => 'acltest-secret',
        ];
        $nodes  = Cluster::bootstrap($configuration)->nodes();
        $stream = reset($nodes)->getConnection($configuration);
        new ReadShareGroupStateRequest(self::ABSENT_GROUP, $this->onePartition(), self::CLIENT_ID, 4807)
            ->writeTo($stream);
        $partition = ReadShareGroupStateResponse::unpack($stream)->results[0]->partitions[0];

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $partition->errorCode);
        self::assertSame('Cluster authorization failed.', $partition->errorMessage);
    }

    /**
     * The partition 0 of the topic of this class, with the leader epoch -1 that a read never writes back
     *
     * @return list<ReadShareGroupStateRequestTopic>
     */
    private function onePartition(): array
    {
        return [
            new ReadShareGroupStateRequestTopic(
                (string) self::$topicId,
                [new ReadShareGroupStateRequestPartition(0, -1)]
            ),
        ];
    }
}
