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
use Protocol\Kafka\Protocol\Data\FetchResponseNodeEndpoint;
use Protocol\Kafka\Protocol\Data\ProduceResponseCurrentLeader;
use Protocol\Kafka\Protocol\Data\ProduceResponseNodeEndpoint;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV15;
use Protocol\Kafka\Protocol\Request\FetchRequestV16;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV15;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequestV10;
use Protocol\Kafka\Protocol\Request\ProduceRequestV9;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponseV10;
use Protocol\Kafka\Protocol\Request\ProduceResponseV9;

/**
 * What **Kafka 3.7** adds to the two apis of the data path: the leader discovery of KIP-951.
 *
 * **Produce v10** and **Fetch v16** leave the request alone - both JSON files @ 3.7.2 say "the same as the version
 * below (KIP-951)" - and give the *answer* the address of the leader a refused partition really has: the tagged
 * `current_leader` of a partition entry, which names the node id and the leader epoch, and the tagged
 * `node_endpoints` of the body, which names the host and the port of every node such an entry points at. A client
 * that reads both can re-send the batch, or move its fetch, without asking Metadata first.
 *
 * A broker writes them for the codes a client has to **move** for: 6 `NotLeaderForPartition` in a produce answer,
 * 6 and 74 `FencedLeaderEpoch` in a fetch answer, and for no other. The one broker of this node is the leader of
 * every partition it hosts, so a *produce* of it can never be answered 6 and the produce half of the KIP is
 * documented on the wire alone; the *fetch* half is reachable through the follower fetch of KIP-903, which the
 * node refuses with exactly that 6 - and then names itself.
 *
 * Every topic of this class is named `t2-37-…`, so that it can run next to the other suites on the shared node.
 *
 * @see docs/protocol/3.9.md, sections "The leader discovery of KIP-951 (v10)", "The leader discovery of KIP-951
 *      (v16)", "Produce API (key 0, v0 to v11)" and "Fetch API (key 1, v0 to v17)"
 */
#[CoversClass(ProduceRequest::class)]
#[CoversClass(ProduceResponse::class)]
#[CoversClass(ProduceRequestV9::class)]
#[CoversClass(ProduceResponseV9::class)]
#[CoversClass(ProduceRequestV10::class)]
#[CoversClass(ProduceResponseV10::class)]
#[CoversClass(ProduceResponseCurrentLeader::class)]
#[CoversClass(ProduceResponseNodeEndpoint::class)]
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchRequestV15::class)]
#[CoversClass(FetchResponseV15::class)]
#[CoversClass(FetchResponseNodeEndpoint::class)]
#[CoversClass(Client::class)]
final class LeaderDiscoveryApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t2-37';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    private const int PARTITION = 0;

    /**
     * A partition of the topic that does not exist: the one produce error a client of this node can provoke
     */
    private const int MISSING_PARTITION = 99;

    /**
     * The two records every test of this class works with
     *
     * @var list<string>
     */
    private const array RECORDS = ['t2-37 one', 't2-37 two'];

    private static ?Cluster $sharedCluster = null;

    private AdminClient $admin;

    private string $topic;

    private string $topicId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->admin = new AdminClient($this->cluster(), $this->configuration());
        $this->topic = self::uniqueTopicName('t2-37-leader');

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

    public function testAVersionTenProduceIsTheVersionNineFrameWithAnotherApiVersion(): void
    {
        $nine = $this->produceRequest(ProduceRequestV9::class, 3760);
        $ten  = $this->produceRequest(ProduceRequestV10::class, 3760);

        self::assertSame(
            bin2hex((string) $nine),
            substr_replace(bin2hex((string) $ten), '0009', 12, 4),
            'KIP-951 added no field to the request: the api version of the header is the whole difference'
        );

        $answer    = $this->send($ten, ProduceResponseV10::class);
        $partition = $answer->topics[$this->topic]->partitions[self::PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(count(self::RECORDS), $partition->baseOffset, 'the batch was appended behind the first');
        self::assertNull($partition->currentLeader, 'an accepted partition names no leader');
        self::assertSame([], $answer->nodeEndpoints, 'and the body of the answer names no endpoint');
        self::assertSame(
            strlen((string) $this->send($this->produceRequest(ProduceRequestV9::class, 3761), ProduceResponseV9::class)),
            strlen((string) $answer),
            'so the answer of version 10 is the answer of version 9, byte count and all'
        );
    }

    public function testAProduceErrorThatIsNotTheSixCarriesNoLeaderHint(): void
    {
        // The one broker of this node is the LEADER of every partition it hosts, so no produce of a client can be
        // answered 6 NotLeaderForPartition, which is the one code KafkaApis.handleProduceRequest @ 3.9.2 writes
        // the hint for. What a client can provoke is the 3 of a partition that does not exist - and it carries
        // nothing in any of its three tagged-field sections
        $answer = $this->send(
            $this->produceRequest(ProduceRequestV10::class, 3762, self::MISSING_PARTITION),
            ProduceResponseV10::class
        );
        $partition = $answer->topics[$this->topic]->partitions[self::MISSING_PARTITION];

        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $partition->errorCode);
        self::assertSame(-1, $partition->baseOffset);
        self::assertNull($partition->currentLeader, 'the 6 is the only produce error the hint is written for');
        self::assertSame([], $answer->nodeEndpoints);
        self::assertSame(10, ProduceRequestV10::VERSION);
        self::assertSame(10, ProduceResponseV10::VERSION);
    }

    public function testAVersionSixteenFetchIsTheVersionFifteenFrameWithAnotherApiVersion(): void
    {
        $fifteen = $this->fetchRequest(FetchRequestV15::class, 3763);
        $sixteen = $this->fetchRequest(FetchRequest::class, 3763);

        self::assertSame(
            bin2hex((string) $fifteen),
            substr_replace(bin2hex((string) $sixteen), '000f', 12, 4),
            'KIP-951 added no field to the request of this api either'
        );

        $answer    = $this->send($sixteen, FetchResponse::class);
        $partition = $answer->topics[0]->partitions[self::PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame([], $answer->nodeEndpoints, 'an answer that refused nothing names no endpoint');
        self::assertNull($partition->currentLeader);
        self::assertSame(
            self::RECORDS,
            array_map(
                static fn(Record $record): string => (string) $record->value,
                MemoryRecords::fromBuffer((string) $partition->messageSet)->getRecords()
            ),
            'and it carries the records, exactly as the version 15 answer of the same question does'
        );
    }

    public function testAFollowerFetchAtVersionSixteenIsAnsweredWithTheLeaderAndItsEndpoint(): void
    {
        // The one way a client of a one-broker cluster reaches the 6 the KIP writes the hint for: a fetch that
        // claims to be a FOLLOWER the partition does not have, which `Partition.followerReplicaOrThrow` @ 3.9.2
        // refuses. The node then names ITSELF as the current leader, with the endpoint of the listener the
        // request arrived on
        $brokerId = self::clusterBrokers() === [] ? 1 : array_key_first(self::clusterBrokers());
        $answer   = $this->send(
            $this->fetchRequest(FetchRequest::class, 3764, 0, -1, $brokerId, 0),
            FetchResponse::class
        );
        $partition = $answer->topics[0]->partitions[self::PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'never a top-level error');
        self::assertSame(KafkaException::NOT_LEADER_FOR_PARTITION, $partition->errorCode);
        self::assertNotNull($partition->currentLeader, 'version 16 is the first one that fills the tag in');
        self::assertSame($brokerId, $partition->currentLeader->leaderId, 'the node names itself');
        self::assertGreaterThanOrEqual(0, $partition->currentLeader->leaderEpoch);
        self::assertSame(
            [$brokerId],
            array_keys($answer->nodeEndpoints),
            'and the body names that node once, keyed by its id'
        );

        $endpoint = $answer->nodeEndpoints[$brokerId];
        self::assertSame($brokerId, $endpoint->nodeId);
        self::assertNotSame('', $endpoint->host, 'the host of the listener the request arrived on');
        self::assertGreaterThan(0, $endpoint->port);
        self::assertNull($endpoint->rack, 'the broker of this node has no rack');
    }

    public function testTheSameFollowerFetchAtVersionFifteenNamesNoLeaderAtAll(): void
    {
        // `KafkaApis.handleFetchRequest` @ 3.9.2 fills the current_leader in behind an `if (versionId >= 16)`,
        // although the tag itself has been on the wire since version 12: the very same refusal of version 15
        // carries the bare error code and a shorter frame
        $brokerId = self::clusterBrokers() === [] ? 1 : array_key_first(self::clusterBrokers());
        $fifteen  = $this->send(
            $this->fetchRequest(FetchRequestV15::class, 3765, 0, -1, $brokerId, 0),
            FetchResponseV15::class
        );
        $sixteen = $this->send(
            $this->fetchRequest(FetchRequest::class, 3766, 0, -1, $brokerId, 0),
            FetchResponse::class
        );

        self::assertSame(
            KafkaException::NOT_LEADER_FOR_PARTITION,
            $fifteen->topics[0]->partitions[self::PARTITION]->errorCode,
            'the same refusal'
        );
        self::assertNull($fifteen->topics[0]->partitions[self::PARTITION]->currentLeader);
        self::assertSame([], $fifteen->nodeEndpoints, 'no version below 16 has a place for the endpoints');
        self::assertGreaterThan(
            strlen((string) $fifteen),
            strlen((string) $sixteen),
            'the hint is what the version 16 answer is longer by'
        );
    }

    public function testAnEpochFromTheFutureIsAnsweredSeventyFiveWithoutAHint(): void
    {
        // The KIP names two codes, 6 and 74, and the 75 is not one of them: a client whose metadata is AHEAD of
        // the broker has nowhere better to go, so the broker tells it nothing. The 74 itself needs an epoch below
        // the one of the partition, which the log of this node - never re-elected, never left the epoch 0 - has
        // no room for
        $answer    = $this->send($this->fetchRequest(FetchRequest::class, 3767, 0, 5), FetchResponse::class);
        $partition = $answer->topics[0]->partitions[self::PARTITION];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame(KafkaException::UNKNOWN_LEADER_EPOCH, $partition->errorCode);
        self::assertNull($partition->currentLeader, 'the 75 is not a code of KIP-951');
        self::assertSame([], $answer->nodeEndpoints);
        self::assertSame(74, KafkaException::FENCED_LEADER_EPOCH, 'the other code the hint is written for');
    }

    public function testASessionOpenedAtVersionFifteenIsContinuedAtVersionSixteen(): void
    {
        $stream = $this->connect();

        try {
            $opened = $this->send(
                $this->fetchRequest(FetchRequestV15::class, 3768, 0, -1, -1, null, FetchMetadata::initial()),
                FetchResponseV15::class,
                $stream
            );

            self::assertSame(KafkaException::NO_ERROR, $opened->errorCode);
            self::assertNotSame(0, $opened->sessionId, 'the node handed out a session');

            $continued = $this->send(
                $this->fetchRequest(
                    FetchRequest::class,
                    3769,
                    count(self::RECORDS),
                    -1,
                    -1,
                    null,
                    FetchMetadata::newIncremental($opened->sessionId)
                ),
                FetchResponse::class,
                $stream
            );

            self::assertSame(KafkaException::NO_ERROR, $continued->errorCode, 'no 70, no 71, no 106');
            self::assertSame($opened->sessionId, $continued->sessionId, 'the session survived the version bump');
            self::assertSame([], $continued->topics, 'and nothing has changed since the full fetch');
            self::assertSame([], $continued->nodeEndpoints);
        } finally {
            $stream->disconnect();
        }
    }

    public function testTheClientSpeaksBothVersionsOfKafkaThreeSeven(): void
    {
        $client = new Client($this->cluster(), $this->configuration());

        $produced = $client->produce([$this->topic => [self::PARTITION => [new Record('t2-37 three')]]]);
        $fetched  = $client->fetchPartitions([$this->topic => [self::PARTITION => 0]], 250);

        self::assertSame(16, FetchRequestV16::VERSION, 'the version KIP-951 added');
        self::assertSame(17, FetchRequest::VERSION, 'which the directory id of KIP-853 raised to 17');
        self::assertSame(10, ProduceRequestV10::VERSION);
        self::assertSame(
            count(self::RECORDS),
            $produced[$this->topic][self::PARTITION]->baseOffset,
            'the producer of this client writes over the version of KIP-951'
        );
        self::assertSame(
            [...self::RECORDS, 't2-37 three'],
            array_map(
                static fn(Record $record): string => (string) $record->value,
                $fetched[$this->topic][self::PARTITION]->getRecords()
            ),
            'and its consumer path reads them back over the version of KIP-951'
        );
    }

    /**
     * Builds the produce request of this test class in the given version
     *
     * @param class-string<ProduceRequest> $requestClass
     */
    private function produceRequest(
        string $requestClass,
        int $correlationId,
        int $partition = self::PARTITION
    ): ProduceRequest {
        $records = [];
        foreach (self::RECORDS as $index => $value) {
            $records[] = new Record($value)->withCreateTime(1700000000000 + $index * 1000);
        }

        return new $requestClass(
            [$this->topic => [$partition => RecordBatch::fromRecords($records)]],
            1,
            self::REQUEST_TIMEOUT_MS,
            self::CLIENT_ID,
            $correlationId
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
        int $currentLeaderEpoch = -1,
        int $replicaId = -1,
        ?int $replicaEpoch = null,
        ?FetchMetadata $metadata = null
    ): FetchRequest {
        return new $requestClass(
            [$this->topic => [self::PARTITION => [$fetchOffset, $currentLeaderEpoch]]],
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
                3750
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
            ProducerConfig::TIMEOUT_MS              => self::REQUEST_TIMEOUT_MS,

            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration();
    }
}
