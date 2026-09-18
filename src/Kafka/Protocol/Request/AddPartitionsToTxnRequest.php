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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnTransaction;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * AddPartitionsToTxn, version 4: enrols topic-partitions into the open transaction (ApiKey 24, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   AddPartitionsToTxn Request (Version: 0 to 3) => transactional_id producer_id producer_epoch [topics]
 *     transactional_id => STRING
 *     producer_id      => INT64
 *     producer_epoch   => INT16
 *     topics           => topic [partitions]
 *       topic      => STRING
 *       partitions => INT32
 *
 *   AddPartitionsToTxn Request (Version: 4)      => [transactions]
 *     transactions => transactional_id producer_id producer_epoch verify_only [topics]
 *       -- since version 4, in place of the four top-level fields
 * </pre>
 *
 * The request has to reach the **transaction coordinator** of the transactional id
 * ({@see \Protocol\Kafka\Client::getTransactionCoordinator()}) and it has to be answered **before the first Produce
 * request** that writes into one of the partitions it names. It is what makes a transaction more than a flag on a
 * record batch: the coordinator writes the partition into the `__transaction_state` entry of the id, and that list
 * is the one it walks when the transaction ends, to write a COMMIT or an ABORT marker into every partition of it. A
 * partition that was never added would receive no marker, so a 0.11.0.3 broker refuses a *transactional* batch of a
 * partition that is not part of the open transaction with the error code **48** (`InvalidTxnState`).
 *
 * The first call of a transaction is also what *starts* it on the broker:
 * `TransactionCoordinator.handleAddPartitionsToTransaction` @ 0.11.0.3 moves the id from `Empty`, `CompleteCommit`
 * or `CompleteAbort` into `Ongoing` and stamps it with the start time that `transaction.timeout.ms` is counted
 * from. There is no "BeginTransaction" request in the protocol at all -
 * {@see \Protocol\Kafka\Producer\KafkaProducer::beginTransaction()} is a purely client-side state change.
 *
 * A partition may be added twice; the coordinator answers the error code 0 for one it already holds. Adding a
 * partition while the previous transaction of the id is still being completed is answered with **51**
 * (`ConcurrentTransactions`), which is retriable after a back-off.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `ADD_PARTITIONS_TO_TXN_REQUEST_V1 =
 * ADD_PARTITIONS_TO_TXN_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see AddPartitionsToTxnRequestV0} is the same frame with the version field of Kafka 0.11.
 *
 * **Kafka 3.5 added the version 4 for the BROKERS, not for the clients** (KIP-890).
 * `AddPartitionsToTxnRequest.json` @ 3.5.2: *"Version 4 adds VerifyOnly field to check if partitions are already in
 * transaction and adds support to batch multiple transactions. Versions 3 and below will be exclusively used by
 * clients and versions 4 and above will be used by brokers."* The four top-level fields become an array of
 * {@see AddPartitionsToTxnTransaction}, each entry with a `verify_only` flag of its own, and the answer grows a
 * top-level error code with one result per transaction ({@see AddPartitionsToTxnResponse}). It is the frame a
 * partition leader sends to the transaction coordinator before it appends a transactional batch, to find out
 * whether the partition really is part of the transaction the batch claims - the verification that
 * `transaction.partition.verification.enable` switches on and that closed the hanging-transaction hole of KIP-890.
 *
 * **This client builds the version 4 but never sends it**, and the node of this line is what says why:
 *
 * * `KafkaApis.handleAddPartitionsToTxnRequest` @ 3.9.2 answers `if (version >= 4)
 *   authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)` - the frame is authorized as a **broker**. The
 *   PLAINTEXT principal of the container is `ANONYMOUS`, which its `super.users` list makes a super user, so the
 *   node answers it there; the SASL user `acltest`, the one principal of the node that is not a super user, is
 *   refused with the **top-level error code 31** (`ClusterAuthorizationFailed`) and an empty
 *   `results_by_transaction` - 13 bytes behind the size field. A deployment that grants `CLUSTER_ACTION` to its
 *   brokers alone answers every client the same way.
 * * A version 4 carries **no authorization of the producer at all**: the versions below it authorize `WRITE` on the
 *   `TRANSACTIONAL_ID` and on every topic of the request, and the version 4 path skips both
 *   (`authorizedTopics = partitionsToAdd.map(_.topic).toSet`), because the broker that sends it has already done
 *   it. A client that sent it would ask the coordinator to trust it.
 * * Its error codes are the ones a **broker** understands: a partition that is not in the transaction is answered
 *   **120** (`TransactionAbortable`) on a 3.9.2 node and a stale epoch **90**, and it is the requesting broker that
 *   maps them to the 48 and the 47 a producer expects (`AddPartitionsToTxnManager.addTxnData` @ 3.9.2 maps the
 *   top-level 31 to a 48 as well, "The client should not be exposed to CLUSTER_AUTHORIZATION_FAILED").
 *
 * {@see \Protocol\Kafka\Client::addPartitionsToTxn()} therefore keeps sending {@see AddPartitionsToTxnRequestV3},
 * the frame of Kafka 2.8, and the version 4 lives here for the wire: its vectors are the frames the node answered.
 * The version **5** of Kafka 3.8, which only promises that the sender understands the code 120, is the first one a
 * client may send again, and it belongs to the KIP-890 wave of this line.
 *
 * @see docs/protocol/3.9.md, section "AddPartitionsToTxn API (key 24, v0 to v4)"
 */
class AddPartitionsToTxnRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ADD_PARTITIONS_TO_TXN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 4;

    /**
     * The version 3 of Kafka 2.8 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * The first version that names several transactions in one request, and the first one a broker sends (Kafka 3.5)
     */
    public const int MIN_BATCHED_VERSION = 4;

    /**
     * Partitions to enrol into the transaction, indexed by the topic name
     *
     * Only the versions below 4 carry it; a batched request names the topics of each transaction of its batch
     * instead.
     *
     * @var array<string, PartitionsForTopic>
     */
    protected readonly array $topics;

    /**
     * The transactions of a batched request, indexed by the transactional id
     *
     * @since Version 4 of protocol
     *
     * @var array<string, AddPartitionsToTxnTransaction>
     */
    protected readonly array $transactions;

    /**
     * @param string $transactionalId `transactional.id` of the producer that owns the transaction
     * @param int    $producerId      Producer id the coordinator handed out for that transactional id
     * @param int    $producerEpoch   Epoch of that producer id, which fences every older incarnation
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Partitions to add, as topic => partitions
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     * @param bool   $verifyOnly      Whether the version 4 only asks if the partitions are in the transaction
     * @param array<string, AddPartitionsToTxnTransaction>|null $transactions The batch of version 4; null - the
     *        default - names `$transactionalId` alone, see {@see self::forTransactions()}
     */
    public function __construct(
        /**
         * The transactional id whose transaction the partitions belong to
         */
        protected readonly string $transactionalId,
        /**
         * Current producer id in use by the transactional id
         */
        protected readonly int $producerId,
        /**
         * Current epoch associated with the producer id
         */
        protected readonly int $producerEpoch,
        array $topicPartitions = [],
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Whether the request only asks if the partitions are in the transaction instead of adding them
         *
         * The `verify_only` of KIP-890, which only the version 4 carries: a request that sets it changes nothing
         * at all and answers the code 0 for a partition the coordinator holds and the 120 for one it does not.
         *
         * @since Version 4 of protocol
         */
        protected readonly bool $verifyOnly = false,
        ?array $transactions = null
    ) {
        $packedTopics = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopics[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values(array_map(intval(...), $partitions)));
        }
        $this->topics = $packedTopics;

        $this->transactions = $transactions ?? [
            $transactionalId => new AddPartitionsToTxnTransaction(
                $transactionalId,
                $producerId,
                $producerEpoch,
                $this->topics,
                $verifyOnly
            ),
        ];

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the batched request of version 4 (Kafka 3.5, KIP-890): several transactions in one frame
     *
     * The frame a **broker** sends: one entry per transactional id, each with its own producer id, epoch,
     * `verify_only` flag and topics. A client of this package has no use for it - the node authorizes the version
     * 4 as `CLUSTER_ACTION` and refuses every principal that is not a broker with the top-level error code 31, see
     * the class docblock - and the builder exists so that the frames of the specification can be built and read.
     *
     * **An empty batch is refused.** A 3.9.2 node answers a `transactions = []` frame with nothing at all -
     * `KafkaApis.handleAddPartitionsToTxnRequest` sends the answer once every transaction of the batch has one,
     * which zero transactions never do - and leaves the connection owing an answer that never comes, which strands
     * every later request on it, exactly as the empty `groups` array of an OffsetFetch does.
     *
     * @param list<AddPartitionsToTxnTransaction> $transactions The transactions of the batch
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     *
     * @throws InvalidRequestException If the batch is empty
     * @throws UnsupportedVersionException If a version below 4 is given more than one transaction
     */
    public static function forTransactions(
        array $transactions,
        string $clientId = '',
        int $correlationId = 0
    ): static {
        if ($transactions === []) {
            throw new InvalidRequestException(
                [
                    'error' => 'An AddPartitionsToTxn request has to name at least one transaction: a broker of '
                        . 'Kafka 3.9.2 answers an empty `transactions` array with nothing at all and strands the '
                        . 'connection',
                ]
            );
        }
        if (static::VERSION < self::MIN_BATCHED_VERSION && count($transactions) > 1) {
            throw new UnsupportedVersionException(
                [
                    'error' => sprintf(
                        'The version %d of the AddPartitionsToTxn api names one transaction per request, the '
                        . '`transactions` array arrived with the version %d in Kafka 3.5',
                        static::VERSION,
                        self::MIN_BATCHED_VERSION
                    ),
                    'transactionalIds' => implode(
                        ', ',
                        array_map(static fn(AddPartitionsToTxnTransaction $transaction): string
                            => $transaction->transactionalId, $transactions)
                    ),
                ]
            );
        }

        $indexed = [];
        foreach ($transactions as $transaction) {
            $indexed[$transaction->transactionalId] = $transaction;
        }
        $first = $transactions[array_key_first($transactions)];

        return new static(
            $first->transactionalId,
            $first->producerId,
            $first->producerEpoch,
            $first->topics,
            $clientId,
            $correlationId,
            $first->verifyOnly,
            $indexed
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        if (static::VERSION >= self::MIN_BATCHED_VERSION) {
            return $header + [
                'transactions' => ['transactionalId' => AddPartitionsToTxnTransaction::class],
            ];
        }

        return $header + [
            'transactionalId' => BinarySchema::TYPE_STRING,
            'producerId'      => BinarySchema::TYPE_INT64,
            'producerEpoch'   => BinarySchema::TYPE_INT16,
            'topics'          => ['topic' => PartitionsForTopic::class],
        ];
    }
}
