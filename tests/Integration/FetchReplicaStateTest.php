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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\FetchRequestReplicaState;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV13;
use Protocol\Kafka\Protocol\Request\FetchRequestV14;
use Protocol\Kafka\Protocol\Request\FetchRequestV15;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV13;
use Protocol\Kafka\Protocol\Request\FetchResponseV14;
use Protocol\Kafka\Protocol\Request\FetchResponseV15;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

/**
 * What **Kafka 3.5** adds to the Fetch api: the error code of KIP-405 and the replica state of KIP-903.
 *
 * **Version 14** adds no field at all and one promise - that the client understands **109**
 * `OffsetMovedToTieredStorage`, which a broker answers for a fetch offset that has moved to remote storage. The
 * node of this line has no remote storage, so what is measured here is the condition itself: an offset past the
 * end of the log is the ordinary **1** `OffsetOutOfRange` at the versions 13, 14 and 15 alike.
 *
 * **Version 15** deprecates the top-level `replica_id` - `"versions": "0-14"` in `FetchRequest.json` @ 3.5.2 - and
 * replaces it with the tagged `replica_state` of a replica id **and a replica epoch**. A consumer is the default
 * `-1` / `-1` of that structure, which a tagged field does not write at all, so a version 15 consumer fetch is the
 * version 14 frame minus the four bytes of the old field. A frame that really names a replica id is a *follower*
 * fetch, and the one broker of this node is the leader of every partition and not one of its followers: such a
 * request is answered per partition, **75** `UnknownLeaderEpoch` when it named a `current_leader_epoch` and **6**
 * `NotLeaderForPartition` when it did not.
 *
 * Every topic of this class is named `t2-35-…`, so that it can run next to the other suites on the shared node.
 *
 * @see docs/protocol/4.3.md, sections "The tiered-storage error of KIP-405 (v14)", "The replica state of KIP-903
 *      (v15)" and "Fetch API (key 1, v0 to v17)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchRequestV13::class)]
#[CoversClass(FetchResponseV13::class)]
#[CoversClass(FetchRequestV14::class)]
#[CoversClass(FetchRequestV15::class)]
#[CoversClass(FetchResponseV14::class)]
#[CoversClass(FetchResponseV15::class)]
#[CoversClass(FetchRequestReplicaState::class)]
#[CoversClass(Client::class)]
final class FetchReplicaStateTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-35';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    private const int PARTITION = 0;

    /**
     * The three records every test of this class fetches
     *
     * @var list<string>
     */
    private const array RECORDS = ['t2-35 one', 't2-35 two', 't2-35 three'];

    private static ?Cluster $sharedCluster = null;

    private AdminClient $admin;

    private string $topic;

    private string $topicId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->admin = new AdminClient($this->cluster(), $this->configuration());
        $this->topic = self::uniqueTopicName('t2-35-replica');

        self::assertSame([$this->topic => null], $this->admin->createTopics([new NewTopic($this->topic, 1, 1)]));
        $this->awaitTopic($this->topic);

        $topicId = $this->cluster()->topicIdOf($this->topic);
        self::assertNotNull($topicId, 'a topic of a KRaft node always has an id');
        $this->topicId = $topicId;

        $this->produce(self::RECORDS);
    }

    protected function tearDown(): void
    {
        if (isset($this->topic)) {
            try {
                $this->admin->deleteTopics([$this->topic]);
            } catch (KafkaException) {
                // A node that can not delete the topic right now must not fail the test that just passed
            }
        }

        parent::tearDown();
    }

    public function testAVersionFourteenFetchIsTheVersionThirteenFrameWithAnotherApiVersion(): void
    {
        $thirteen = $this->fetchRequest(FetchRequestV13::class, 3501);
        $fourteen = $this->fetchRequest(FetchRequestV14::class, 3501);

        self::assertSame(
            bin2hex((string) $thirteen),
            substr_replace(bin2hex((string) $fourteen), '000d', 12, 4),
            'KIP-405 added no field to the request: the api version of the header is the whole difference'
        );

        $answer = $this->send($fourteen, FetchResponseV14::class);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        $partition = $answer->topics[0]->partitions[self::PARTITION];
        self::assertSame($this->topicId, $answer->topics[0]->topicId, 'the answer names the id of KIP-516');
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(count(self::RECORDS), $partition->highWaterMarkOffset);
        self::assertNotSame('', $partition->messageSet, 'the records of the log came back');
    }

    public function testAnOffsetPastTheEndOfTheLogIsTheOrdinaryOutOfRangeAtEveryVersionOfKafkaThreeFive(): void
    {
        // The condition a broker with remote storage answers with the 109 OffsetMovedToTieredStorage of KIP-405:
        // an offset that is not in the local log. This node has no remote storage - and no offset of it can have
        // moved anywhere - so every version answers the 1 that made the two indistinguishable before the KIP
        foreach (
            [
                [FetchRequestV13::class, FetchResponseV13::class],
                [FetchRequestV14::class, FetchResponseV14::class],
                [FetchRequest::class, FetchResponse::class],
            ] as $index => [$requestClass, $responseClass]
        ) {
            $answer = $this->send(
                $this->fetchRequest($requestClass, 3510 + $index, 99),
                $responseClass
            );

            $partition = $answer->topics[0]->partitions[self::PARTITION];
            self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'never a top-level error');
            self::assertSame(
                KafkaException::OFFSET_OUT_OF_RANGE,
                $partition->errorCode,
                "the version {$requestClass} is answered the ordinary 1"
            );
            self::assertSame(-1, $partition->highWaterMarkOffset);
            self::assertSame('', (string) $partition->messageSet);
        }

        self::assertSame(109, KafkaException::OFFSET_MOVED_TO_TIERED_STORAGE, 'the code version 14 exists for');
    }

    public function testAVersionFifteenConsumerFetchCarriesNoReplicaIdAtAll(): void
    {
        $fourteen = $this->fetchRequest(FetchRequestV14::class, 3520);
        $fifteen  = $this->fetchRequest(FetchRequestV15::class, 3520);

        self::assertSame(15, $fifteen->getApiVersion());
        self::assertNull($fifteen->getReplicaState(), 'a consumer writes no replica state');
        self::assertSame(-1, $fifteen->getReplicaId());
        self::assertSame(
            strlen((string) $fourteen) - 4,
            strlen((string) $fifteen),
            'the four bytes of the deprecated replica_id are gone from the body (KIP-903)'
        );
        self::assertStringEndsWith('0100', bin2hex((string) $fifteen), 'the empty rack, then an EMPTY tag buffer');

        $answer = $this->send($fifteen, FetchResponseV15::class);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        $partition = $answer->topics[0]->partitions[self::PARTITION];
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(count(self::RECORDS), $partition->highWaterMarkOffset);
        self::assertSame(
            self::RECORDS,
            array_map(
                static fn(Record $record): string => (string) $record->value,
                MemoryRecords::fromBuffer((string) $partition->messageSet)->getRecords()
            ),
            'the answer of version 15 is the answer of version 14, records and all'
        );
    }

    public function testAFetchThatClaimsToBeAReplicaIsRefusedByTheReplicaSetAndNotByItsEpoch(): void
    {
        // The one broker of the node is the LEADER of this partition and not one of its followers, so
        // `Partition.followerReplicaOrThrow` @ 3.9.2 finds no replica to record a position for, whatever epoch the
        // request carries. Which code it answers depends on the partition entry alone
        $brokerId = self::clusterBrokers() === [] ? 1 : array_key_first(self::clusterBrokers());

        foreach ([[$brokerId, 0], [$brokerId, -1], [$brokerId, 100000], [4242, 5]] as $index => [$replica, $epoch]) {
            $request = $this->fetchRequest(FetchRequest::class, 3530 + $index, 0, $replica, $epoch);

            self::assertSame($replica, $request->getReplicaId());
            self::assertSame($epoch, $request->getReplicaState()?->replicaEpoch);
            self::assertStringEndsWith(
                '010d' . bin2hex(pack('N', $replica)) . bin2hex(pack('J', $epoch)) . '00',
                bin2hex((string) $request),
                'the tagged replica state is the last thing in the frame'
            );

            $answer    = $this->send($request, FetchResponse::class);
            $partition = $answer->topics[0]->partitions[self::PARTITION];

            self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'never a top-level error');
            self::assertSame(
                KafkaException::NOT_LEADER_FOR_PARTITION,
                $partition->errorCode,
                'a follower fetch of a replica the partition does not have is the 6'
            );
            self::assertSame(-1, $partition->highWaterMarkOffset);
            self::assertSame(-1, $partition->logStartOffset);
            self::assertSame('', (string) $partition->messageSet);
        }
    }

    public function testAFollowerFetchThatNamesTheLeaderEpochIsAnsweredUnknownLeaderEpoch(): void
    {
        // The other half of `Partition.followerReplicaOrThrow`: with a current_leader_epoch in the partition entry
        // the broker answers 75 instead of 6 - "the tuple (replicaId, leaderEpoch) is not yet recognized as valid"
        $brokerId = self::clusterBrokers() === [] ? 1 : array_key_first(self::clusterBrokers());
        $request  = new FetchRequest(
            [$this->topic => [self::PARTITION => [0, 0]]],
            250,
            1,
            1048576,
            $brokerId,
            self::CLIENT_ID,
            3540,
            52428800,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            [$this->topic => $this->topicId],
            0
        );

        $answer    = $this->send($request, FetchResponse::class);
        $partition = $answer->topics[0]->partitions[self::PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame(KafkaException::UNKNOWN_LEADER_EPOCH, $partition->errorCode);
        self::assertSame(-1, $partition->highWaterMarkOffset);
    }

    public function testTheDebuggingReplicaIdIsServedLikeAConsumerFetch(): void
    {
        // -2 asks to read like a follower without being one; `FetchRequest.isFromFollower` @ 3.9.2 is
        // `replicaId >= 0`, so the request is served as a consumer's although it writes a replica state
        $request = $this->fetchRequest(
            FetchRequest::class,
            3550,
            0,
            OffsetsRequest::DEBUGGING_REPLICA_ID
        );

        self::assertSame(-2, $request->getReplicaState()?->replicaId);
        self::assertSame(-1, $request->getReplicaState()?->replicaEpoch);

        $partition = $this->send($request, FetchResponse::class)->topics[0]->partitions[self::PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(count(self::RECORDS), $partition->highWaterMarkOffset);
        self::assertNotSame('', (string) $partition->messageSet);
    }

    public function testASessionOpenedAtVersionThirteenIsContinuedAtVersionFifteen(): void
    {
        // Raising the version inside a fetch session is not the 106 FetchSessionTopicIdError of Kafka 3.1: what
        // that code fences is a session whose topics change the KIND of name they travel under
        $stream = $this->connect();

        try {
            $opened = $this->send(
                new FetchRequestV13(
                    [$this->topic => [self::PARTITION => 0]],
                    250,
                    1,
                    1048576,
                    -1,
                    self::CLIENT_ID,
                    3560,
                    52428800,
                    FetchRequest::READ_UNCOMMITTED,
                    FetchMetadata::initial(),
                    [],
                    FetchRequest::NO_RACK,
                    null,
                    [$this->topic => $this->topicId]
                ),
                FetchResponseV13::class,
                $stream
            );

            self::assertSame(KafkaException::NO_ERROR, $opened->errorCode);
            self::assertNotSame(0, $opened->sessionId, 'the node handed out a session');

            $continued = $this->send(
                new FetchRequest(
                    [$this->topic => [self::PARTITION => count(self::RECORDS)]],
                    250,
                    1,
                    1048576,
                    -1,
                    self::CLIENT_ID,
                    3561,
                    52428800,
                    FetchRequest::READ_UNCOMMITTED,
                    FetchMetadata::newIncremental($opened->sessionId),
                    [],
                    FetchRequest::NO_RACK,
                    null,
                    [$this->topic => $this->topicId]
                ),
                FetchResponse::class,
                $stream
            );

            self::assertSame(KafkaException::NO_ERROR, $continued->errorCode, 'no 106, no 70, no 71');
            self::assertSame($opened->sessionId, $continued->sessionId, 'the session survived the version bump');
            self::assertSame([], $continued->topics, 'and nothing changed since the full fetch');
        } finally {
            $stream->disconnect();
        }
    }

    public function testTheClientAndTheConsumerSpeakVersionFifteen(): void
    {
        $client  = new Client($this->cluster(), $this->configuration());
        $fetched = $client->fetchPartitions([$this->topic => [self::PARTITION => 0]], 250);

        // Kafka 3.7 raised the version this client sends to 16 (KIP-951) and Kafka 3.9 to 17 (KIP-853), both
        // of them the version 15 frame of this test with another number in its header; `FetchRequestV15`
        // keeps the version the class measures
        self::assertSame(15, FetchRequestV15::VERSION);
        self::assertSame(17, FetchRequest::VERSION);
        self::assertSame(
            self::RECORDS,
            array_map(
                static fn(Record $record): string => (string) $record->value,
                $fetched[$this->topic][self::PARTITION]->getRecords()
            ),
            'the consumer path of this client reads its records over the version of KIP-903'
        );
    }

    /**
     * Builds the fetch of this test class in the given version
     *
     * @param class-string<FetchRequest> $requestClass
     */
    private function fetchRequest(
        string $requestClass,
        int $correlationId,
        int $fetchOffset = 0,
        int $replicaId = -1,
        ?int $replicaEpoch = null
    ): FetchRequest {
        return new $requestClass(
            [$this->topic => [self::PARTITION => $fetchOffset]],
            250,
            1,
            1048576,
            $replicaId,
            self::CLIENT_ID,
            $correlationId,
            52428800,
            FetchRequest::READ_UNCOMMITTED,
            null,
            [],
            FetchRequest::NO_RACK,
            null,
            [$this->topic => $this->topicId],
            $replicaEpoch
        );
    }

    /**
     * Appends the records of this test class to the partition under test
     *
     * @param list<string> $values
     */
    private function produce(array $values): void
    {
        $records = [];
        foreach ($values as $index => $value) {
            $records[] = new Record($value)->withCreateTime(1700000000000 + $index * 1000);
        }

        $answer = $this->send(
            new ProduceRequest(
                [$this->topic => [self::PARTITION => RecordBatch::fromRecords($records)]],
                1,
                self::REQUEST_TIMEOUT_MS,
                self::CLIENT_ID,
                3500
            ),
            ProduceResponse::class
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $answer->topics[$this->topic]->partitions[self::PARTITION]->errorCode,
            'the records of this test could not be produced'
        );
    }

    /**
     * Sends one request and reads the answer that belongs to it, optionally over a connection that is kept open
     *
     * @template T of AbstractResponse
     *
     * @param class-string<T> $responseClass Class the answer is decoded with
     *
     * @return T
     */
    private function send(
        AbstractRequest $request,
        string $responseClass,
        ?SocketStream $stream = null
    ): AbstractResponse {
        $stream ??= $this->connect();
        $request->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    /**
     * Waits until the fresh partition has a leader that answers, which a brand new topic needs a moment for
     */
    private function awaitTopic(string $topic): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster()->reload();
                $this->admin->listOffsets([$topic => [self::PARTITION]], OffsetsRequest::LATEST);

                return;
            } catch (KafkaException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed> Client configuration of this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,

            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,

            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration();
    }
}
