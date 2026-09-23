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

use Closure;
use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\ConsumerGroupMetadata;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnRequestV3;
use Protocol\Kafka\Protocol\Request\AddOffsetsToTxnResponse;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV3;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV4;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponse;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\EndTxnRequestV3;
use Protocol\Kafka\Protocol\Request\EndTxnResponse;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequest;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequestV4;
use Protocol\Kafka\Protocol\Request\InitProducerIdResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitRequestV3;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\TxnOffsetCommitResponseV3;

/**
 * Measures the five version bumps of KIP-890 part 2 (Kafka 3.8) against the 3.9.2 KRaft node.
 *
 * `InitProducerIdRequest.json`, `AddPartitionsToTxnRequest.json`, `AddOffsetsToTxnRequest.json`,
 * `EndTxnRequest.json` and `TxnOffsetCommitRequest.json` @ 3.8.1 all declare the new version with the same
 * sentence - *"adds support for new error code TRANSACTION_ABORTABLE (KIP-890)"* - and not one field, so what
 * there is to measure is what the node answers at the higher number:
 *
 * * the four versions this client sends (InitProducerId v5, AddOffsetsToTxn v4, EndTxn v4, TxnOffsetCommit v4)
 *   answer exactly what their predecessors answer, error codes included, and never the 120;
 * * AddPartitionsToTxn **v5** stays a broker version, because `KafkaApis.handleAddPartitionsToTxnRequest` @ 3.9.2
 *   authorizes `if (version >= 4)` as `CLUSTER_ACTION` - so the SASL user `acltest` is refused the top-level 31
 *   there as it is at the version 4, and `Client::addPartitionsToTxn()` keeps the version 3;
 * * the **120** is reachable in the `verify_only` path of that very api and nowhere else on this node;
 * * and the node finalizes no `transaction.version` at all, so the behaviour half of KIP-890 part 2 - the epoch
 *   bump per transaction, the implicit enrolment of a partition by a Produce - cannot be reached from here.
 *
 * Every transaction of this class is opened with the version 3 the client sends and ended in the teardown, and
 * every topic it creates is deleted again.
 *
 * @see docs/protocol/4.3.md, sections "The transaction protocol v2 of KIP-890 (Kafka 3.8), and what a client reaches on this node", "InitProducerId API (key 22, v0 to v5)", "AddPartitionsToTxn API (key 24, v0 to v5)", "AddOffsetsToTxn API (key 25, v0 to v4)", "EndTxn API (key 26, v0 to v4)" and "TxnOffsetCommit API (key 28, v0 to v4)"
 */
#[CoversClass(InitProducerIdRequest::class)]
#[CoversClass(InitProducerIdRequestV4::class)]
#[CoversClass(InitProducerIdResponse::class)]
#[CoversClass(AddOffsetsToTxnRequest::class)]
#[CoversClass(AddOffsetsToTxnRequestV3::class)]
#[CoversClass(AddOffsetsToTxnResponse::class)]
#[CoversClass(EndTxnRequest::class)]
#[CoversClass(EndTxnRequestV3::class)]
#[CoversClass(EndTxnResponse::class)]
#[CoversClass(TxnOffsetCommitRequest::class)]
#[CoversClass(TxnOffsetCommitRequestV3::class)]
#[CoversClass(TxnOffsetCommitResponse::class)]
#[CoversClass(TxnOffsetCommitResponseV3::class)]
#[CoversClass(AddPartitionsToTxnRequest::class)]
#[CoversClass(AddPartitionsToTxnRequestV4::class)]
#[CoversClass(AddPartitionsToTxnResponse::class)]
final class Kip890TransactionVersionsTest extends IntegrationTestCase
{
    /**
     * The one SASL user of the image that is not in `super.users`
     */
    private const string UNPRIVILEGED_USER = 'acltest';

    private const string UNPRIVILEGED_PASSWORD = 'acltest-secret';

    /**
     * The code Kafka 3.8 declared and the one a 3.9.2 node answers a `verify_only` of a foreign partition with
     */
    private const int TRANSACTION_ABORTABLE = 120;

    /**
     * How long to wait for a fresh topic to become servable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    private const int TRANSACTION_TIMEOUT_MS = 60000;

    private Cluster $cluster;

    private AdminClient $admin;

    private Client $client;

    /**
     * Topics this test created, deleted again after every test
     *
     * @var list<string>
     */
    private array $createdTopics = [];

    /**
     * Transactions this test opened, as `transactional id => producer id and epoch`, aborted after every test
     *
     * @var array<string, ProducerIdAndEpoch>
     */
    private array $openTransactions = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
        $this->client  = new Client($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        foreach ($this->openTransactions as $transactionalId => $idAndEpoch) {
            try {
                $this->client->endTxn(
                    $this->client->getTransactionCoordinator($transactionalId),
                    $transactionalId,
                    $idAndEpoch,
                    false
                );
            } catch (KafkaException) {
                // the coordinator has already ended it, which is the state this teardown wants
            }
        }
        $this->openTransactions = [];

        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    /**
     * The four versions the client sends are the versions this branch is at
     */
    public function testTheClientSendsTheVersionsKafka38Added(): void
    {
        self::assertSame(5, InitProducerIdRequest::VERSION);
        self::assertSame(4, AddOffsetsToTxnRequest::VERSION);
        self::assertSame(4, EndTxnRequest::VERSION);
        self::assertSame(4, TxnOffsetCommitRequest::VERSION);
        self::assertSame(5, AddPartitionsToTxnRequest::VERSION, 'built, but never sent');
        self::assertSame(3, AddPartitionsToTxnRequestV3::VERSION, 'the version Client::addPartitionsToTxn() sends');
    }

    /**
     * InitProducerId v5: a producer id is handed out, the last epoch is a retry and anything below it is fenced
     */
    public function testTheInitProducerIdVersionFiveHandsOutAnIdAndFencesAStaleEpochWithTheCode90(): void
    {
        $transactionalId = self::transactionalId('init');

        $fresh = $this->exchange(
            new InitProducerIdRequest($transactionalId, self::TRANSACTION_TIMEOUT_MS, -1, -1, self::CLIENT_ID, 3830),
            InitProducerIdResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $fresh->errorCode);
        self::assertGreaterThanOrEqual(0, $fresh->producerId);

        $bumped = $this->exchange(
            new InitProducerIdRequest(
                $transactionalId,
                self::TRANSACTION_TIMEOUT_MS,
                $fresh->producerId,
                $fresh->producerEpoch,
                self::CLIENT_ID,
                3831
            ),
            InitProducerIdResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $bumped->errorCode);
        self::assertSame($fresh->producerId, $bumped->producerId, 'the same id, one epoch higher');
        self::assertSame($fresh->producerEpoch + 1, $bumped->producerEpoch);

        // The epoch that was just bumped away is the LAST one, which the coordinator reads as a retry of the bump
        $retry = $this->exchange(
            new InitProducerIdRequest(
                $transactionalId,
                self::TRANSACTION_TIMEOUT_MS,
                $fresh->producerId,
                $fresh->producerEpoch,
                self::CLIENT_ID,
                3832
            ),
            InitProducerIdResponse::class
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $retry->errorCode,
            '`prepareInitProducerIdTransit` @ 3.9.2 answers the last epoch with the current pair, not with a refusal'
        );
        self::assertSame($bumped->producerEpoch, $retry->producerEpoch);

        // One more bump leaves that epoch two behind, and only then is it fenced
        $this->exchange(
            new InitProducerIdRequest(
                $transactionalId,
                self::TRANSACTION_TIMEOUT_MS,
                $retry->producerId,
                $retry->producerEpoch,
                self::CLIENT_ID,
                3833
            ),
            InitProducerIdResponse::class
        );

        $fenced = $this->exchange(
            new InitProducerIdRequest(
                $transactionalId,
                self::TRANSACTION_TIMEOUT_MS,
                $fresh->producerId,
                $fresh->producerEpoch,
                self::CLIENT_ID,
                3834
            ),
            InitProducerIdResponse::class
        );

        self::assertSame(
            KafkaException::PRODUCER_FENCED,
            $fenced->errorCode,
            'the 90 of KIP-588 is what the version 5 answers a fenced producer, never the 120 of its own KIP'
        );
        self::assertSame(-1, $fenced->producerId);
        self::assertSame(-1, $fenced->producerEpoch);

        // The very same question at the version 4 is answered the very same way
        $atVersionFour = $this->exchange(
            new InitProducerIdRequestV4(
                $transactionalId,
                self::TRANSACTION_TIMEOUT_MS,
                $fresh->producerId,
                $fresh->producerEpoch,
                self::CLIENT_ID,
                3835
            ),
            InitProducerIdResponse::class
        );

        self::assertSame(KafkaException::PRODUCER_FENCED, $atVersionFour->errorCode);
    }

    /**
     * AddOffsetsToTxn v4 and TxnOffsetCommit v4: the offsets of a group inside the transaction, and their refusals
     */
    public function testTheOffsetApisOfTheTransactionAtTheVersionsKafka38Added(): void
    {
        $topic           = $this->topic('offsets', 1);
        $group           = self::uniqueTopicName('t4-38-offsets') . '-group';
        $transactionalId = self::transactionalId('offsets');
        $idAndEpoch      = $this->openTransaction($transactionalId, [$topic => [0]]);

        $added = $this->exchange(
            new AddOffsetsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                $group,
                self::CLIENT_ID,
                3840
            ),
            AddOffsetsToTxnResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $added->errorCode);

        $unknownProducer = $this->exchange(
            new AddOffsetsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId + 1000,
                $idAndEpoch->epoch,
                $group,
                self::CLIENT_ID,
                3841
            ),
            AddOffsetsToTxnResponse::class
        );

        self::assertSame(
            KafkaException::INVALID_PRODUCER_ID_MAPPING,
            $unknownProducer->errorCode,
            'the 49 of the version 0, answered unchanged at the version 4'
        );

        $committed = $this->exchange(
            new TxnOffsetCommitRequest(
                $transactionalId,
                $group,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0 => 42]],
                null,
                self::CLIENT_ID,
                3842
            ),
            TxnOffsetCommitResponse::class
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $committed->topics[$topic]->partitions[0]->errorCode,
            'the generation -1 with an empty member id is the commit of a producer that is not a member'
        );

        $refused = $this->exchange(
            new TxnOffsetCommitRequest(
                $transactionalId,
                $group,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0 => 43]],
                new ConsumerGroupMetadata($group, 99, 't4-38-no-such-member', null),
                self::CLIENT_ID,
                3843
            ),
            TxnOffsetCommitResponse::class
        );

        self::assertSame(
            KafkaException::UNKNOWN_MEMBER_ID,
            $refused->topics[$topic]->partitions[0]->errorCode,
            'the member id is validated before the generation, so this is the 25 and not the 22'
        );
    }

    /**
     * TxnOffsetCommit v4 is the one client api of the five whose refusal the version really changes
     *
     * The group coordinator has the `__consumer_offsets` partition of the group verified against the transaction
     * coordinator before it writes the offsets - the partition verification of KIP-890 part 1 - and
     * `GroupCoordinator.handleTxnCommitOffsets` @ 3.9.2 reports its failure by the version of the request:
     * `transactionSupportedOperation = if (apiVersion >= 4) genericError else defaultError`, and
     * `AddPartitionsToTxnManager` folds the 120 back into a 48 for anything below. A commit that no
     * AddOffsetsToTxn preceded is exactly that failure.
     */
    public function testTheTxnOffsetCommitVersionFourIsAnsweredThe120WhereTheVersionThreeGetsThe48(): void
    {
        $topic = $this->topic('abortable', 1);
        $group = self::uniqueTopicName('t4-38-abortable') . '-group';

        $atVersionFour = $this->exchange(
            new TxnOffsetCommitRequest(
                ...$this->commitOfATransactionWithoutItsOffsetsPartition('v4', $topic, $group, 3870)
            ),
            TxnOffsetCommitResponse::class
        );

        self::assertSame(
            self::TRANSACTION_ABORTABLE,
            $atVersionFour->topics[$topic]->partitions[0]->errorCode,
            'the version 4 promises the code, so the verification failure reaches the client as the 120'
        );

        $atVersionThree = $this->exchange(
            new TxnOffsetCommitRequestV3(
                ...$this->commitOfATransactionWithoutItsOffsetsPartition('v3', $topic, $group, 3873)
            ),
            TxnOffsetCommitResponseV3::class
        );

        self::assertSame(
            KafkaException::INVALID_TXN_STATE,
            $atVersionThree->topics[$topic]->partitions[0]->errorCode,
            'and the very same commit at the version 3 is answered the 48 this client treats as fatal'
        );
    }

    /**
     * Opens a transaction that holds a partition of the topic but NOT the offsets partition of the group
     *
     * @return array{string, string, int, int, array<string, array<int, int>>, null, string, int} The arguments of
     *         a TxnOffsetCommit request of that transaction
     */
    private function commitOfATransactionWithoutItsOffsetsPartition(
        string $purpose,
        string $topic,
        string $group,
        int $correlationId
    ): array {
        $transactionalId = self::transactionalId($purpose);
        $idAndEpoch      = $this->openTransaction($transactionalId, [$topic => [0]]);

        return [
            $transactionalId,
            $group,
            $idAndEpoch->producerId,
            $idAndEpoch->epoch,
            [$topic => [0 => 7]],
            null,
            self::CLIENT_ID,
            $correlationId,
        ];
    }

    /**
     * EndTxn v4: the commit is answered 0, and the abort that follows it the 48 of the state table of Kafka 0.11
     */
    public function testTheEndTxnVersionFourCommitsAndRefusesAnAbortOfACommittedTransaction(): void
    {
        $topic           = $this->topic('end', 1);
        $transactionalId = self::transactionalId('end');
        $idAndEpoch      = $this->openTransaction($transactionalId, [$topic => [0]]);

        $committed = $this->exchange(
            new EndTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                EndTxnRequest::COMMIT,
                self::CLIENT_ID,
                3850
            ),
            EndTxnResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $committed->errorCode);
        unset($this->openTransactions[$transactionalId]);

        $illegal = $this->exchangeUntil(
            fn(int $correlationId): EndTxnRequest => new EndTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                EndTxnRequest::ABORT,
                self::CLIENT_ID,
                $correlationId
            ),
            EndTxnResponse::class,
            3851,
            static fn(EndTxnResponse $answer): bool
                => $answer->errorCode !== KafkaException::CONCURRENT_TRANSACTIONS
        );

        self::assertSame(
            KafkaException::INVALID_TXN_STATE,
            $illegal->errorCode,
            'there is no transition from CompleteCommit to an abort, while a repeated commit is a no-op success'
        );
    }

    /**
     * AddPartitionsToTxn v5: the broker version of Kafka 3.5 with a higher number, the 120 and the top-level 31
     */
    public function testTheAddPartitionsToTxnVersionFiveIsABrokerVersionThatAnswersTheCode120(): void
    {
        $topic           = $this->topic('verify', 2);
        $transactionalId = self::transactionalId('verify');
        $idAndEpoch      = $this->openTransaction($transactionalId, [$topic => [0]]);

        $verified = $this->exchange(
            new AddPartitionsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0, 1]],
                self::CLIENT_ID,
                3860,
                true
            ),
            AddPartitionsToTxnResponse::class
        );

        self::assertSame(KafkaException::NO_ERROR, $verified->errorCode, 'a super user passes CLUSTER_ACTION');

        $partitions = $verified->resultOf($transactionalId)->topicResults[$topic]->partitionErrors;

        self::assertSame(KafkaException::NO_ERROR, $partitions[0]->errorCode);
        self::assertSame(
            self::TRANSACTION_ABORTABLE,
            $partitions[1]->errorCode,
            'the one place a 3.9.2 broker writes the code Kafka 3.8 declared'
        );

        $refused = $this->exchange(
            new AddPartitionsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0]],
                self::CLIENT_ID,
                3861,
                true
            ),
            AddPartitionsToTxnResponse::class,
            $this->unprivilegedStream()
        );

        self::assertSame(
            KafkaException::CLUSTER_AUTHORIZATION_FAILED,
            $refused->errorCode,
            'the authorization of this api is `if (version >= 4)`, so the version 5 is a broker version as well'
        );
        self::assertSame([], $refused->resultsByTransaction);
    }

    /**
     * The node finalizes no `transaction.version`, so the behaviour half of KIP-890 part 2 is out of reach
     */
    public function testTheNodeFinalizesNoTransactionVersion(): void
    {
        $features = $this->admin->describeFeatures();

        self::assertArrayHasKey('metadata.version', $features->finalizedFeatures);
        self::assertArrayNotHasKey(
            'transaction.version',
            $features->finalizedFeatures,
            'TransactionVersion TV_1 and TV_2 both need the metadata version of Kafka 4.0 (IBP_4_0_IV0)'
        );
        self::assertArrayNotHasKey(
            'transaction.version',
            $features->supportedFeatures,
            '`BrokerFeatures.defaultSupportedFeatures` @ 3.9.2 leaves a production feature whose latest production'
            . ' level is 0 out of the map altogether'
        );
    }

    /**
     * Sends one frame on a fresh PLAINTEXT connection - or on the given one - and decodes the answer
     *
     * @template T of object
     *
     * @param class-string<T> $responseClass
     *
     * @return T
     */
    private function exchange(
        AbstractRequest $request,
        string $responseClass,
        ?SocketStream $stream = null
    ): object {
        $stream ??= $this->connect();
        $request->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    /**
     * Repeats an exchange on a fresh connection until the answer satisfies the predicate
     *
     * The coordinator answers 51 `ConcurrentTransactions` while the markers of the transaction that was just
     * committed are still being written, and the io of this client is fast enough to reach it there.
     *
     * @template T of object
     *
     * @param Closure(int): AbstractRequest $build
     * @param class-string<T>               $responseClass
     * @param callable(T): bool             $isFinal
     *
     * @return T
     */
    private function exchangeUntil(
        callable $build,
        string $responseClass,
        int $correlationId,
        callable $isFinal
    ): object {
        $deadline = microtime(true) + 30.0;
        do {
            $answer = $this->exchange($build($correlationId++), $responseClass);
            if ($isFinal($answer)) {
                return $answer;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        return $answer;
    }

    /**
     * Runs `InitProducerId` for the id and opens its transaction with the version 3 frame the client sends
     *
     * @param array<string, list<int>> $topicPartitions Partitions the transaction starts with
     */
    private function openTransaction(string $transactionalId, array $topicPartitions): ProducerIdAndEpoch
    {
        $idAndEpoch = $this->client->initProducerId($transactionalId, self::TRANSACTION_TIMEOUT_MS);
        $this->openTransactions[$transactionalId] = $idAndEpoch;

        $this->client->addPartitionsToTxn(
            $this->client->getTransactionCoordinator($transactionalId),
            $transactionalId,
            $idAndEpoch,
            $topicPartitions
        );

        return $idAndEpoch;
    }

    /**
     * A transactional id that no other test of the shared node uses
     *
     * A transactional id can not be deleted - the coordinator forgets it when `transactional.id.expiration.ms` is
     * up - so every one of them is unique per run and prefixed with the wave this class belongs to.
     */
    private static function transactionalId(string $purpose): string
    {
        return 't4-38-' . $purpose . '-txn-' . bin2hex(random_bytes(6));
    }

    /**
     * Creates a topic for this test and waits until the node serves every partition of it
     */
    private function topic(string $purpose, int $partitions): string
    {
        $topic                 = self::uniqueTopicName("t4-38-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, $partitions, 1)]));

        // A fresh topic of this node answers 3, 5 or 6 for a moment, until its leader is elected and known here
        $wanted   = range(0, $partitions - 1);
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster->reload();
                $this->admin->listOffsets([$topic => $wanted], OffsetsRequest::LATEST);

                return $topic;
            } catch (KafkaException | TopicPartitionRequestException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    /**
     * A connection of the one SASL principal of the node that is not a super user
     */
    private function unprivilegedStream(): SocketStream
    {
        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, the refusal needs a principal');
        }

        return new SocketStream(
            'tcp://' . self::saslBootstrapServer(),
            [
                ClientConfig::SECURITY_PROTOCOL  => SecurityProtocol::SASL_PLAINTEXT,
                ClientConfig::SASL_MECHANISM     => SaslMechanism::PLAIN,
                ClientConfig::SASL_USERNAME      => self::UNPRIVILEGED_USER,
                ClientConfig::SASL_PASSWORD      => self::UNPRIVILEGED_PASSWORD,
                ClientConfig::CLIENT_ID          => self::CLIENT_ID,
                ClientConfig::REQUEST_TIMEOUT_MS => 10000,
            ],
            5.0
        );
    }

    /**
     * Client configuration pointing at the node under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Client id of every frame of this class, the one the wire vectors of the 3.8 wave carry
     */
    private const string CLIENT_ID = 'kafka-client-t4-38';
}
