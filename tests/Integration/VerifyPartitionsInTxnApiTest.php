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
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnTransaction;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequest;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequestV3;
use Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Measures the AddPartitionsToTxn **v4** of Kafka 3.5 (KIP-890) against the 3.9.2 node.
 *
 * The version is the one a **broker** sends: `AddPartitionsToTxnRequest.json` @ 3.5.2 says *"Versions 3 and below
 * will be exclusively used by clients and versions 4 and above will be used by brokers"*, and
 * `KafkaApis.handleAddPartitionsToTxnRequest` @ 3.9.2 enforces it with
 * `if (version >= 4) authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)` in place of the `WRITE`
 * authorization of the transactional id and of the topics that every version below asks for. A partition leader
 * sends it to find out whether a partition really is part of the transaction whose batch it is about to append -
 * the verification `transaction.partition.verification.enable` switches on.
 *
 * This client therefore builds the frame and never sends it: {@see Client::addPartitionsToTxn()} keeps the
 * {@see AddPartitionsToTxnRequestV3} of Kafka 2.8. What this class measures is the node's own behaviour, and why
 * that decision is the right one:
 *
 * * on the PLAINTEXT listener, where `ANONYMOUS` is in the `super.users` of the image, the frame is answered;
 * * as the SASL user `acltest`, the one principal of the node that is not a super user, the whole request is
 *   refused with the **top-level 31** - which is what a client is answered on any cluster whose authorizer gives
 *   `CLUSTER_ACTION` to the brokers alone;
 * * and the per-partition codes are broker codes: **120** for a partition the transaction does not hold, where a
 *   producer is told the 48.
 *
 * Every transaction of this class is opened with the version 3 the client sends, so the two halves are measured
 * against each other, and aborted again in the teardown.
 *
 * @see docs/protocol/3.9.md, section "AddPartitionsToTxn API (key 24, v0 to v4)"
 */
#[CoversClass(AddPartitionsToTxnRequest::class)]
#[CoversClass(AddPartitionsToTxnRequestV3::class)]
#[CoversClass(AddPartitionsToTxnResponse::class)]
#[CoversClass(AddPartitionsToTxnTransaction::class)]
final class VerifyPartitionsInTxnApiTest extends IntegrationTestCase
{
    /**
     * The one SASL user of the image that is not in `super.users`
     */
    private const string UNPRIVILEGED_USER = 'acltest';

    private const string UNPRIVILEGED_PASSWORD = 'acltest-secret';

    /**
     * The code a 3.9.2 coordinator answers a partition that is not part of the transaction with (Kafka 3.8)
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
                // the coordinator has already aborted it, which is the state this teardown wants
            }
        }
        $this->openTransactions = [];

        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    /**
     * `verify_only` answers what the transaction holds and changes nothing
     */
    public function testAVerificationAnswersZeroForAPartitionOfTheTransactionAndTheCode120ForAnyOther(): void
    {
        $topic          = $this->topic('verify', 2);
        $transactionalId = self::transactionalId('verify');
        $idAndEpoch     = $this->openTransaction($transactionalId, [$topic => [0]]);

        $answer = $this->exchange(
            new AddPartitionsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0, 1]],
                't4-35-verify',
                3540,
                true
            )
        );

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'a super user passes CLUSTER_ACTION');
        $partitions = $answer->resultOf($transactionalId)->topicResults[$topic]->partitionErrors;
        self::assertSame(
            KafkaException::NO_ERROR,
            $partitions[0]->errorCode,
            'the partition the version 3 request added is part of the transaction'
        );
        self::assertSame(
            self::TRANSACTION_ABORTABLE,
            $partitions[1]->errorCode,
            '`handleVerifyPartitionsInTransaction` @ 3.9.2 answers a partition that is not in it with the 120,'
            . ' which the broker that asked maps to the 48 before the producer of the batch sees it'
        );

        // and the verification changed nothing at all: the same question is answered the same way
        $again = $this->exchange(
            new AddPartitionsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [1]],
                't4-35-verify',
                3541,
                true
            )
        );
        self::assertSame(
            self::TRANSACTION_ABORTABLE,
            $again->resultOf($transactionalId)->topicResults[$topic]->partitionErrors[1]->errorCode
        );
    }

    /**
     * The same version with the flag off is the request the versions 0 to 3 are, and it really adds the partition
     */
    public function testTheVersion4AddsThePartitionWhenVerifyOnlyIsFalse(): void
    {
        $topic           = $this->topic('add', 2);
        $transactionalId = self::transactionalId('add');
        $idAndEpoch      = $this->openTransaction($transactionalId, [$topic => [0]]);

        $added = $this->exchange(
            new AddPartitionsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [1]],
                't4-35-add',
                3542
            )
        );
        self::assertSame(
            KafkaException::NO_ERROR,
            $added->resultOf($transactionalId)->topicResults[$topic]->partitionErrors[1]->errorCode
        );

        $verified = $this->exchange(
            new AddPartitionsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0, 1]],
                't4-35-add',
                3543,
                true
            )
        );
        foreach ($verified->resultOf($transactionalId)->topicResults[$topic]->partitionErrors as $partition) {
            self::assertSame(
                KafkaException::NO_ERROR,
                $partition->errorCode,
                'both partitions belong to the transaction now'
            );
        }
    }

    /**
     * The batch of the version: two transactional ids in one frame, each answered with the topics it asked about
     */
    public function testTheVersion4AnswersEveryTransactionOfABatch(): void
    {
        $topic = $this->topic('batch', 2);
        $first  = self::transactionalId('batch-one');
        $second = self::transactionalId('batch-two');
        $firstProducer  = $this->openTransaction($first, [$topic => [0]]);
        $secondProducer = $this->openTransaction($second, [$topic => [0]]);

        $answer = $this->exchange(
            AddPartitionsToTxnRequest::forTransactions(
                [
                    new AddPartitionsToTxnTransaction(
                        $first,
                        $firstProducer->producerId,
                        $firstProducer->epoch,
                        [$topic => [0, 1]],
                        true
                    ),
                    new AddPartitionsToTxnTransaction(
                        $second,
                        $secondProducer->producerId,
                        $secondProducer->epoch,
                        [$topic => [1]]
                    ),
                ],
                't4-35-batch',
                3544
            )
        );

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertCount(2, $answer->resultsByTransaction, 'one entry per transaction of the request');

        $verification = $answer->resultOf($first)->topicResults[$topic]->partitionErrors;
        self::assertSame(KafkaException::NO_ERROR, $verification[0]->errorCode);
        self::assertSame(self::TRANSACTION_ABORTABLE, $verification[1]->errorCode);

        self::assertSame(
            KafkaException::NO_ERROR,
            $answer->resultOf($second)->topicResults[$topic]->partitionErrors[1]->errorCode,
            'the second transaction of the batch really added its partition'
        );
    }

    /**
     * The measurement this wave decided on: a principal that is not a broker is refused the whole request
     */
    public function testAPrincipalWithoutClusterActionIsRefusedTheWholeRequest(): void
    {
        $topic           = $this->topic('refused', 1);
        $transactionalId = self::transactionalId('refused');
        $idAndEpoch      = $this->openTransaction($transactionalId, [$topic => [0]]);

        $unprivileged = $this->unprivilegedStream();
        new AddPartitionsToTxnRequest(
            $transactionalId,
            $idAndEpoch->producerId,
            $idAndEpoch->epoch,
            [$topic => [0]],
            't4-35-refused',
            3545,
            true
        )->writeTo($unprivileged);
        $refused = AddPartitionsToTxnResponse::unpack($unprivileged);

        self::assertSame(3545, $refused->getCorrelationId());
        self::assertSame(
            KafkaException::CLUSTER_AUTHORIZATION_FAILED,
            $refused->errorCode,
            'every version from 4 on is authorized as CLUSTER_ACTION, which no client principal holds'
        );
        self::assertSame([], $refused->resultsByTransaction, 'and not one transaction is answered with that code');
    }

    /**
     * The version this client sends stays the 3, and the node still serves it next to the version 4
     */
    public function testTheClientKeepsSendingTheVersionThree(): void
    {
        self::assertSame(3, AddPartitionsToTxnRequestV3::VERSION);
        self::assertSame(4, AddPartitionsToTxnRequest::VERSION, 'the class of the version 4 exists for the wire');

        $topic           = $this->topic('v3', 1);
        $transactionalId = self::transactionalId('v3');
        $idAndEpoch      = $this->openTransaction($transactionalId, [$topic => [0]]);

        // `Client::addPartitionsToTxn()` sent the version 3 above, and the version 4 confirms what it did
        $answer = $this->exchange(
            new AddPartitionsToTxnRequest(
                $transactionalId,
                $idAndEpoch->producerId,
                $idAndEpoch->epoch,
                [$topic => [0]],
                't4-35-v3',
                3546,
                true
            )
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $answer->resultOf($transactionalId)->topicResults[$topic]->partitionErrors[0]->errorCode
        );
    }

    /**
     * Sends one frame on a fresh PLAINTEXT connection and decodes the answer
     */
    private function exchange(AddPartitionsToTxnRequest $request): AddPartitionsToTxnResponse
    {
        $stream = $this->connect();
        $request->writeTo($stream);

        return AddPartitionsToTxnResponse::unpack($stream);
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
     * A transactional id can not be deleted - the coordinator forgets it when
     * `transactional.id.expiration.ms` is up - so every one of them is unique per run and prefixed with the
     * ticket of this wave.
     */
    private static function transactionalId(string $purpose): string
    {
        return 't4-35-' . $purpose . '-txn-' . bin2hex(random_bytes(6));
    }

    /**
     * Creates a topic for this test and waits until the node serves every partition of it
     */
    private function topic(string $purpose, int $partitions): string
    {
        $topic                 = self::uniqueTopicName("t4-35-{$purpose}");
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
                ClientConfig::CLIENT_ID          => 't4-35-refused',
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
            ClientConfig::CLIENT_ID                 => 't4-35-verify',
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,
        ] + ProducerConfig::getDefaultConfiguration() + ConsumerConfig::getDefaultConfiguration();
    }
}
