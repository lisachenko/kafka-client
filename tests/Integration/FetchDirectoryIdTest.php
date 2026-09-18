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
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV16;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV16;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

/**
 * What **Kafka 3.9** adds to the Fetch api: the replica directory id of KIP-853 (version 17).
 *
 * `FetchRequest.json` @ 3.9.2 declares one field for the version, a `ReplicaDirectoryId` uuid inside the
 * **partition** entry with `"taggedVersions": "17+", "tag": 0`, and `FetchResponse.json` declares none at all -
 * "Version 17 no changes to the response (KIP-853)". The field names the log **directory** a follower keeps its
 * replica of that partition in, the `directory.id` a KRaft node writes into the `meta.properties` of every one of
 * its `log.dirs`, and it exists so that the metadata quorum of KIP-853 can identify a voter by more than its node
 * id while its membership changes.
 *
 * Two things follow, and both are measured here:
 *
 * * a **consumer** never writes it. The zero uuid is the default of the field, and a tagged field whose value is
 *   its default is left off the wire altogether, so a version 17 consumer fetch is the version 16 frame with
 *   another number in its header;
 * * a fetch of an **ordinary topic** never reads it. `KafkaRaftClient.handleFetchRequest` @ 3.9.2 builds the
 *   `ReplicaKey` of the fetching replica out of the replica id and this directory id, and that handler serves
 *   `__cluster_metadata` on the controller listener; `ReplicaManager.fetchMessages` @ 3.9.2, which serves every
 *   other fetch, does not look at the field. A directory id that exists nowhere, the real id of a log directory
 *   of the node and none at all are therefore answered byte for byte the same.
 *
 * Every topic of this class is named `t2-39-…`, so that it can run next to the other suites on the shared node.
 *
 * @see docs/protocol/3.9.md, sections "The replica directory id of KIP-853 (v17)" and "Fetch API (key 1, v0 to v17)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchRequestV16::class)]
#[CoversClass(FetchResponseV16::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(Client::class)]
final class FetchDirectoryIdTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-39';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    private const int PARTITION = 0;

    /**
     * The records every test of this class fetches
     *
     * @var list<string>
     */
    private const array RECORDS = ['t2-39 one', 't2-39 two'];

    private static ?Cluster $sharedCluster = null;

    private AdminClient $admin;

    private string $topic;

    private string $topicId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->admin = new AdminClient($this->cluster(), $this->configuration());
        $this->topic = self::uniqueTopicName('t2-39-directory');

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

    public function testAVersionSeventeenConsumerFetchIsTheVersionSixteenFrameWithAnotherApiVersion(): void
    {
        $sixteen   = $this->fetchRequest(FetchRequestV16::class, 3900);
        $seventeen = $this->fetchRequest(FetchRequest::class, 3900);

        self::assertSame(17, $seventeen->getApiVersion());
        self::assertSame(
            Uuid::ZERO,
            $seventeen->getReplicaDirectoryId(),
            'a consumer names no log directory, and the zero uuid is the default of the field'
        );
        self::assertSame(
            bin2hex((string) $sixteen),
            substr_replace(bin2hex((string) $seventeen), '0010', 12, 4),
            'KIP-853 added a TAGGED field: the api version of the header is the whole difference'
        );
        self::assertStringEndsWith('0100', bin2hex((string) $seventeen), 'the empty rack, then an EMPTY tag buffer');

        $answer    = $this->send($seventeen, FetchResponse::class);
        $partition = $answer->topics[0]->partitions[self::PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame($this->topicId, $answer->topics[0]->topicId, 'the answer names the id of KIP-516');
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(count(self::RECORDS), $partition->highWaterMarkOffset);
        self::assertSame(
            self::RECORDS,
            array_map(
                static fn(Record $record): string => (string) $record->value,
                MemoryRecords::fromBuffer((string) $partition->messageSet)->getRecords()
            ),
            'and the answer of version 17 is the answer of version 16, records and all'
        );
    }

    public function testADirectoryIdCostsEighteenBytesAndChangesNothingAboutTheAnswer(): void
    {
        // The tag 0 of the partition entry: the tag, its size and the 16 raw bytes of the uuid, with the tag
        // buffer of the entry counting 01 instead of 00. The node parses it and hands the records out as always
        $directoryId = random_bytes(Uuid::SIZE);
        $plain       = $this->fetchRequest(FetchRequest::class, 3901);
        $named       = $this->fetchRequest(FetchRequest::class, 3901, 0, -1, null, $directoryId);

        self::assertSame($directoryId, $named->getReplicaDirectoryId());
        self::assertSame(strlen((string) $plain) + 18, strlen((string) $named));
        self::assertStringContainsString(
            '01' . '00' . '10' . bin2hex($directoryId),
            bin2hex((string) $named),
            'the tag 0 of the partition entry, its size 16 and the directory id'
        );

        $withoutTag = $this->send($plain, FetchResponse::class);
        $withTag    = $this->send($named, FetchResponse::class);

        self::assertSame(
            bin2hex((string) $withoutTag),
            bin2hex((string) $withTag),
            'a directory id in a fetch of an ordinary topic is parsed and never read'
        );
    }

    public function testAFollowerFetchThatNamesADirectoryIsRefusedExactlyAsOneThatDoesNot(): void
    {
        // `Partition.followerReplicaOrThrow` @ 3.9.2 refuses the claim of a replica the partition does not have
        // before anything else is read: the one broker of this node is the LEADER of the partition. Neither the
        // replica epoch of KIP-903 nor the directory of KIP-853 is ever reached
        $brokerId    = self::clusterBrokers() === [] ? 1 : array_key_first(self::clusterBrokers());
        $directoryId = random_bytes(Uuid::SIZE);

        foreach (
            [
                [-1, KafkaException::NOT_LEADER_FOR_PARTITION],
                [0, KafkaException::UNKNOWN_LEADER_EPOCH],
            ] as $index => [$leaderEpoch, $expected]
        ) {
            $named = $this->fetchRequest(
                FetchRequest::class,
                3910 + $index,
                $leaderEpoch === -1 ? 0 : [0, $leaderEpoch],
                $brokerId,
                0,
                $directoryId
            );
            $plain = $this->fetchRequest(
                FetchRequest::class,
                3910 + $index,
                $leaderEpoch === -1 ? 0 : [0, $leaderEpoch],
                $brokerId,
                0
            );

            $withTag    = $this->send($named, FetchResponse::class);
            $withoutTag = $this->send($plain, FetchResponse::class);

            self::assertSame(KafkaException::NO_ERROR, $withTag->errorCode, 'never a top-level error');
            self::assertSame($expected, $withTag->topics[0]->partitions[self::PARTITION]->errorCode);
            self::assertSame(
                bin2hex((string) $withoutTag),
                bin2hex((string) $withTag),
                'and the answer is the one of the very same frame without a directory id'
            );
        }
    }

    public function testASessionOpenedAtVersionSixteenIsContinuedAtVersionSeventeen(): void
    {
        // Raising the version inside a fetch session is no more an error at 16 -> 17 than it was at 15 -> 16
        $stream = $this->connect();

        try {
            $opened = $this->send(
                $this->fetchRequest(FetchRequestV16::class, 3920, 0, -1, null, null, FetchMetadata::initial()),
                FetchResponseV16::class,
                $stream
            );

            self::assertSame(KafkaException::NO_ERROR, $opened->errorCode);
            self::assertNotSame(0, $opened->sessionId, 'the node handed out a session');

            $continued = $this->send(
                $this->fetchRequest(
                    FetchRequest::class,
                    3921,
                    count(self::RECORDS),
                    -1,
                    null,
                    null,
                    FetchMetadata::newIncremental($opened->sessionId)
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

    public function testTheClientAndTheConsumerSpeakVersionSeventeen(): void
    {
        $client  = new Client($this->cluster(), $this->configuration());
        $fetched = $client->fetchPartitions([$this->topic => [self::PARTITION => 0]], 250);

        self::assertSame(16, FetchRequestV16::VERSION);
        self::assertSame(17, FetchRequest::VERSION, 'the version this client sends since Kafka 3.9');
        self::assertSame(17, FetchResponse::VERSION);
        self::assertSame(
            self::RECORDS,
            array_map(
                static fn(Record $record): string => (string) $record->value,
                $fetched[$this->topic][self::PARTITION]->getRecords()
            ),
            'the consumer path of this client reads its records over the version of KIP-853'
        );
    }

    /**
     * Builds the fetch of this test class in the given version
     *
     * @param class-string<FetchRequest> $requestClass
     * @param int|array{int, int}        $fetchOffset  The offset, or the pair `[offset, currentLeaderEpoch]`
     */
    private function fetchRequest(
        string $requestClass,
        int $correlationId,
        int|array $fetchOffset = 0,
        int $replicaId = -1,
        ?int $replicaEpoch = null,
        ?string $replicaDirectoryId = null,
        ?FetchMetadata $metadata = null
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
            $metadata,
            [],
            FetchRequest::NO_RACK,
            null,
            [$this->topic => $this->topicId],
            $replicaEpoch,
            $replicaDirectoryId
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
                3890
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
