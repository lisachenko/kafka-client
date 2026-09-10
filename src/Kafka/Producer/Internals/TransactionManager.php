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

namespace Protocol\Kafka\Producer\Internals;

use Closure;
use LogicException;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Errors\ConcurrentTransactionsException;
use Protocol\Kafka\Common\Errors\DuplicateSequenceNumberException;
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
use Protocol\Kafka\Common\Errors\GroupLoadInProgressException;
use Protocol\Kafka\Common\Errors\InvalidPidMappingException;
use Protocol\Kafka\Common\Errors\InvalidTxnStateException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NotCoordinatorForGroupException;
use Protocol\Kafka\Common\Errors\OutOfOrderSequenceException;
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Errors\TransactionalIdAuthorizationException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequest;
use Throwable;

/**
 * The producer state of KIP-98: one producer id with its epoch, and the sequence number of every topic-partition.
 *
 * This is the client half of the idempotent producer. The broker deduplicates an appended batch by the triple
 * (producer id, partition, sequence range), so a producer that wants "exactly once, in order" has to hand out
 * **gapless sequence numbers per topic-partition** and to re-send a batch that was not acknowledged **with the very
 * same numbers**; then a retry after a lost acknowledgement is recognised by the broker as the batch it already
 * holds, and is answered with the offset of the original append instead of appending it a second time.
 *
 * The name is the one of the Java client, and so is the shape of the API: the class holds the *whole* producer
 * state of KIP-98, of which the idempotent producer uses the first half - the producer id, the epoch and the
 * sequence numbers. The **transactional** half sits on exactly those members: a manager that carries a
 * `transactional.id` ({@see TransactionManager::isTransactional()}) asks its **transaction coordinator** for the
 * producer id instead of any broker, keeps the {@see TransactionState} of the producer, remembers which
 * topic-partitions are part of the open transaction, and sends the four requests of the transaction protocol -
 * `AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit` and `EndTxn`. The public entry points of that half
 * are {@see TransactionManager::initTransactions()}, {@see TransactionManager::beginTransaction()},
 * {@see TransactionManager::maybeAddPartitionsToTransaction()},
 * {@see TransactionManager::sendOffsetsToTransaction()}, {@see TransactionManager::commitTransaction()} and
 * {@see TransactionManager::abortTransaction()}, which {@see \Protocol\Kafka\Producer\KafkaProducer} exposes
 * under the very same names.
 *
 * ### The coordinator, and what is retried
 *
 * Every request of the transaction protocol but `TxnOffsetCommit` goes to the **transaction coordinator** of the
 * transactional id, `TxnOffsetCommit` to the **group coordinator** of the consumer group whose offsets it commits;
 * both are looked up once and kept until a broker says they are wrong. The three codes that say "ask again" - 14
 * `GroupLoadInProgress`, 15 `GroupCoordinatorNotAvailable` and 16 `NotCoordinatorForGroup` - make this class look
 * the coordinator up again, and **51 `ConcurrentTransactions`**, the code of a coordinator that is still
 * completing the previous transaction of the id, is simply retried after `retry.backoff.ms`. None of the four
 * says that the request failed, so their budget is a **deadline** rather than the `retries` of a batch:
 * `metadata.fetch.timeout.ms`, the same one a coordinator lookup uses, see {@see TransactionManager::retrying()}.
 *
 * ### What the broker does with those numbers, and what this class does with its answers
 *
 * Verified against the 0.11.0.3 container, `ProducerStateManager.validateAppend` @ 0.11.0.3:
 *
 * | The broker sees                                        | It answers                    | This class then      |
 * |--------------------------------------------------------|-------------------------------|----------------------|
 * | the next sequence of that producer and partition        | 0 and the new base offset     | increments by the record count |
 * | *exactly* the batch it appended last (id, epoch, range) | **0 and the original offset** | increments as if it had just been written |
 * | a sequence with a gap, or a duplicate of an *older* batch | **45** OutOfOrderSequence    | {@see TransactionManager::resetProducerId()} - a new producer id for the next send |
 * | an epoch below the one it has for that producer id      | **47** InvalidProducerEpoch   | fatal, the producer refuses to send anything else |
 * | a duplicate that its last-batch check did not catch     | 46 DuplicateSequenceNumber    | counts the batch as appended, the sequence is consumed |
 *
 * The error code 46 is the one a 0.11.0.3 broker never actually sends to a client - its duplicate check runs
 * *before* the sequence validation and answers the duplicate with a normal offset - so the row above is what this
 * class does when it does arrive, from a broker that behaves otherwise.
 *
 * Resetting the producer id on a 45 is what the Java `Sender` @ 0.11.0.3 does for an **idempotent** producer, and
 * it is the only sensible answer: a gap in the sequence means that the producer and the broker no longer agree on
 * what was written, and there is no frame that could fill the gap. The batch that hit it is reported to the caller,
 * everything after it starts a new producer id at the sequence 0 - and loses the deduplication of everything that
 * was written under the old id. A *transactional* producer must not do this (bumping the epoch behind the back of
 * an open transaction), which is why {@see TransactionManager::resetProducerId()} refuses it.
 *
 * @see \Protocol\Kafka\Client::initProducerId()
 * @see docs/protocol/1.1.md, sections "InitProducerId API (key 22, v0)" and "The idempotent producer"
 */
class TransactionManager
{
    /**
     * The first sequence number of every topic-partition; `RecordBatch.NO_SEQUENCE + 1` in the Java client
     */
    public const int FIRST_SEQUENCE = 0;

    /**
     * The largest sequence number of the wire, an int32; the next one wraps around to {@see self::FIRST_SEQUENCE}
     */
    public const int MAX_SEQUENCE = 0x7FFFFFFF;

    /**
     * How long the requests of the transaction protocol are repeated while a coordinator says "ask again", when the
     * configuration carries no `metadata.fetch.timeout.ms`
     */
    public const int DEFAULT_COORDINATOR_TIMEOUT_MS = 30000;

    /**
     * Producer id and epoch of this producer, {@see ProducerIdAndEpoch::none()} until the first `InitProducerId`
     */
    private ProducerIdAndEpoch $producerIdAndEpoch;

    /**
     * Sequence number the next batch of a topic-partition starts at, as topic => partition => sequence
     *
     * @var array<string, array<int, int>>
     */
    private array $sequenceNumbers = [];

    /**
     * The error that made this producer unusable, `null` while it is healthy
     */
    private ?Throwable $fatalError = null;

    /**
     * The error of the state this producer is in, for {@see TransactionState::ABORTABLE_ERROR} as well as for
     * {@see TransactionState::FATAL_ERROR}; `null` in every state that is not an error
     */
    private ?Throwable $lastError = null;

    /**
     * Where in its transaction this producer is, `UNINITIALIZED` until `initTransactions()` was called
     */
    private TransactionState $currentState = TransactionState::UNINITIALIZED;

    /**
     * Transaction coordinator of the transactional id, `null` until it was looked up
     */
    private ?Node $transactionCoordinator = null;

    /**
     * Group coordinator of every consumer group offsets were committed for, by group id
     *
     * @var array<string, Node>
     */
    private array $groupCoordinators = [];

    /**
     * Topic-partitions the coordinator already knows as part of the open transaction, by `topic-partition`
     *
     * @var array<string, TopicPartition>
     */
    private array $partitionsInTransaction = [];

    /**
     * Topic-partitions that still have to be sent to the coordinator, by `topic-partition`
     *
     * @var array<string, TopicPartition>
     */
    private array $newPartitionsInTransaction = [];

    /**
     * @param Client      $client               Client that sends the `InitProducerId` request
     * @param string|null $transactionalId      `transactional.id` of the producer, `null` for a merely idempotent
     *        one; a non-null id makes the producer id survive a restart and is looked up at the transaction
     *        coordinator of that id
     * @param int         $transactionTimeoutMs `transaction.timeout.ms`, only meaningful with a transactional id
     * @param array<string, mixed> $configuration Client options the `retries` and `retry.backoff.ms` of the
     *        coordinator retries are taken from; the defaults of {@see RetryPolicy} apply to an empty one
     */
    public function __construct(
        private readonly Client $client,
        private readonly ?string $transactionalId = null,
        private readonly int $transactionTimeoutMs = InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS,
        private readonly array $configuration = []
    ) {
        $this->producerIdAndEpoch = ProducerIdAndEpoch::none();
    }

    /**
     * Tells whether this producer writes transactions, i.e. whether it was configured with a `transactional.id`
     */
    public function isTransactional(): bool
    {
        return $this->transactionalId !== null;
    }

    /**
     * Returns the `transactional.id` of this producer, `null` for a merely idempotent one
     */
    public function getTransactionalId(): ?string
    {
        return $this->transactionalId;
    }

    /**
     * Returns the `transaction.timeout.ms` that an `InitProducerId` of this producer states
     */
    public function getTransactionTimeoutMs(): int
    {
        return $this->transactionTimeoutMs;
    }

    /**
     * Tells whether a producer id was handed out to this producer already
     */
    public function hasProducerId(): bool
    {
        return $this->producerIdAndEpoch->isValid();
    }

    /**
     * Returns the producer id and the epoch that every batch of this producer carries
     */
    public function getProducerIdAndEpoch(): ProducerIdAndEpoch
    {
        return $this->producerIdAndEpoch;
    }

    /**
     * Tells whether a batch that was written with these two values still belongs to the current producer.
     *
     * A batch that does not is a batch of a producer id that {@see TransactionManager::resetProducerId()} threw
     * away in the meantime: its acknowledgement must not touch the sequence numbers of the new id, and its failure
     * must not reset that id again.
     */
    public function hasProducerIdAndEpoch(int $producerId, int $epoch): bool
    {
        return $this->producerIdAndEpoch->matches($producerId, $epoch);
    }

    /**
     * Replaces the producer id and the epoch of this producer, e.g. with what an `InitProducerId` answered
     */
    public function setProducerIdAndEpoch(ProducerIdAndEpoch $producerIdAndEpoch): void
    {
        $this->producerIdAndEpoch = $producerIdAndEpoch;
    }

    /**
     * Returns the producer id of this producer and asks a broker for one if it does not have one yet.
     *
     * This is `Sender.maybeWaitForProducerId()` of the Java client: the producer id is fetched lazily, with the
     * first batch that is about to be sent, so that constructing a producer costs no request at all.
     *
     * @throws KafkaException If the producer is in a fatal state, or if the broker refuses to hand out an id
     */
    public function maybeInitProducerId(): ProducerIdAndEpoch
    {
        $this->maybeThrowFatalError();

        if (!$this->producerIdAndEpoch->isValid()) {
            $this->producerIdAndEpoch = $this->client->initProducerId(
                $this->transactionalId,
                $this->transactionTimeoutMs
            );
        }

        return $this->producerIdAndEpoch;
    }

    /**
     * Throws away the producer id and every sequence number of this producer.
     *
     * The next batch then asks for a new producer id and starts at the sequence 0 again. This is what an
     * idempotent producer does when it can not know any more whether a batch was written - an out-of-order
     * sequence, or a batch that ran out of retries - because a producer that kept its id would have every
     * following batch rejected with the same error.
     *
     * @throws \LogicException For a transactional producer, whose producer id may only be re-initialized by an
     *         `InitProducerId` at its coordinator, which would silently abort an open transaction
     */
    public function resetProducerId(): void
    {
        if ($this->isTransactional()) {
            throw new \LogicException(
                'Can not reset the producer state of a transactional producer: abort the ongoing transaction or '
                . 'initialize the producer again instead'
            );
        }

        $this->producerIdAndEpoch = ProducerIdAndEpoch::none();
        $this->sequenceNumbers    = [];
    }

    /**
     * Returns the sequence number that the next batch of this topic-partition starts at
     */
    public function sequenceNumber(TopicPartition $topicPartition): int
    {
        return $this->sequenceNumbers[$topicPartition->topic][$topicPartition->partition] ??= self::FIRST_SEQUENCE;
    }

    /**
     * Moves the sequence of a topic-partition on by the number of records that were appended to it.
     *
     * The sequence space is an **unsigned** int32 that wraps around, exactly as
     * {@see RecordBatch::getLastSequence()} computes the last sequence of a batch, so a producer that writes more
     * than two billion records into one partition keeps working.
     */
    public function incrementSequenceNumber(TopicPartition $topicPartition, int $increment): void
    {
        $current = $this->sequenceNumber($topicPartition);
        $next    = $current + $increment;
        if ($next > self::MAX_SEQUENCE) {
            $next -= self::MAX_SEQUENCE + 1;
        }

        $this->sequenceNumbers[$topicPartition->topic][$topicPartition->partition] = $next;
    }

    /**
     * Returns the sequence number of every topic-partition of a produce request, as {@see Client::produce()} wants
     * them.
     *
     * @param array<string, array<int, mixed>> $topicPartitions Whatever is keyed by topic and partition, e.g. the
     *        buffered records of a batch
     *
     * @return array<string, array<int, int>> topic => partition => sequence the batch starts at
     */
    public function baseSequences(array $topicPartitions): array
    {
        $baseSequences = [];
        foreach ($topicPartitions as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                $baseSequences[$topic][$partition] = $this->sequenceNumber(
                    new TopicPartition((string) $topic, (int) $partition)
                );
            }
        }

        return $baseSequences;
    }

    /**
     * Records that the broker acknowledged a batch of this many records, which consumes their sequence numbers.
     *
     * A batch that was written with another producer id - one that a reset threw away while it was in flight - is
     * ignored, exactly as `Sender.completeBatch()` @ 0.11.0.3 ignores it.
     */
    public function batchCompleted(TopicPartition $topicPartition, int $recordCount, ?ProducerIdAndEpoch $batchIdAndEpoch = null): void
    {
        if ($batchIdAndEpoch !== null && !$this->hasProducerIdAndEpoch($batchIdAndEpoch->producerId, $batchIdAndEpoch->epoch)) {
            return;
        }
        if (!$this->hasProducerId()) {
            return;
        }

        $this->incrementSequenceNumber($topicPartition, $recordCount);
    }

    /**
     * Reacts to the error that a topic-partition of a produce request came back with.
     *
     * The three codes of KIP-98 are the ones that say something about the *producer state* rather than about the
     * partition, and each of them has exactly one answer:
     *
     * * **47** InvalidProducerEpoch ({@see ProducerFencedException}) - another producer took this id over, or the
     *   coordinator expired the transaction. Nothing this producer sends will ever be accepted again, so it goes
     *   into a fatal state and every following call fails with the same error.
     * * **45** OutOfOrderSequence - the producer and the broker do not agree on what is in the log any more. An
     *   idempotent producer throws its producer id away and starts over; a transactional one reports the error and
     *   leaves the decision to the caller, which has to abort the transaction.
     * * **46** DuplicateSequenceNumber - the batch is already in the log, so its sequence numbers are consumed
     *   and the producer moves on as if it had just written them.
     *
     * Every other error - a lost leader, an authorization failure, a timeout - says nothing about the producer
     * state and is left to the caller. **For a producer inside a transaction it says one thing more**: the batch
     * that failed may or may not be in the log, so the transaction can not be committed any more and the producer
     * goes into {@see TransactionState::ABORTABLE_ERROR}, from which only
     * {@see TransactionManager::abortTransaction()} leads out. That is what `Sender.failBatch()` @ 0.11.0.3 does
     * with `transactionManager.maybeTransitionToErrorState()`.
     */
    public function batchFailed(
        TopicPartition $topicPartition,
        Throwable $error,
        int $recordCount = 0,
        ?ProducerIdAndEpoch $batchIdAndEpoch = null
    ): void {
        if ($batchIdAndEpoch !== null && !$this->hasProducerIdAndEpoch($batchIdAndEpoch->producerId, $batchIdAndEpoch->epoch)) {
            return;
        }

        if ($error instanceof ProducerFencedException) {
            $this->transitionToFatalError($error);

            return;
        }
        if ($error instanceof DuplicateSequenceNumberException) {
            $this->incrementSequenceNumber($topicPartition, $recordCount);

            return;
        }
        if ($error instanceof OutOfOrderSequenceException && !$this->isTransactional() && $this->hasProducerId()) {
            $this->resetProducerId();
        }
        if ($this->isTransactional() && $this->currentState === TransactionState::IN_TRANSACTION) {
            $this->transitionToAbortableError($error);
        }
    }

    /**
     * Puts the producer into the state it can not leave: every following call reports this error.
     */
    public function transitionToFatalError(Throwable $exception): void
    {
        $this->fatalError = $exception;
        $this->lastError  = $exception;
        $this->currentState = TransactionState::FATAL_ERROR;
    }

    /**
     * Puts the producer into the state that only an abort leads out of.
     *
     * The transaction can not be committed any more - a batch of it may or may not be in the log - but the
     * producer itself is intact: `abortTransaction()` rolls the transaction back and the next `beginTransaction()`
     * starts a new one with the same producer id. A producer that is already aborting stays where it is, exactly
     * as `TransactionManager.transitionToAbortableError()` @ 0.11.0.3 skips the transition then.
     */
    public function transitionToAbortableError(Throwable $exception): void
    {
        if ($this->currentState === TransactionState::ABORTING_TRANSACTION) {
            return;
        }

        $this->transitionTo(TransactionState::ABORTABLE_ERROR, $exception);
    }

    /**
     * Tells whether this producer hit an error it can not recover from
     */
    public function hasFatalError(): bool
    {
        return $this->fatalError !== null;
    }

    /**
     * Returns the error that made this producer unusable, `null` while it is healthy
     */
    public function lastFatalError(): ?Throwable
    {
        return $this->fatalError;
    }

    /**
     * Reports the fatal error of this producer, if it has one
     *
     * @throws Throwable The error the producer died of
     */
    public function maybeThrowFatalError(): void
    {
        if ($this->fatalError !== null) {
            throw $this->fatalError;
        }
    }

    /**
     * Returns the state of the transaction of this producer
     */
    public function currentState(): TransactionState
    {
        return $this->currentState;
    }

    /**
     * Returns the error of the state this producer is in, `null` while it is healthy
     */
    public function lastError(): ?Throwable
    {
        return $this->lastError;
    }

    /**
     * Tells whether this producer is in one of the two error states
     */
    public function hasError(): bool
    {
        return $this->currentState->isError();
    }

    /**
     * Tells whether this producer hit an error that an abort of the transaction cleans up
     */
    public function hasAbortableError(): bool
    {
        return $this->currentState === TransactionState::ABORTABLE_ERROR;
    }

    /**
     * Tells whether a transaction is open right now
     */
    public function isInTransaction(): bool
    {
        return $this->currentState === TransactionState::IN_TRANSACTION;
    }

    /**
     * Tells whether this producer is initialized and has no transaction open
     */
    public function isReady(): bool
    {
        return $this->currentState === TransactionState::READY;
    }

    /**
     * Tells whether a topic-partition is part of the open transaction already
     */
    public function isPartitionAdded(TopicPartition $topicPartition): bool
    {
        return isset($this->partitionsInTransaction[(string) $topicPartition]);
    }

    /**
     * Initializes the transactional producer: fences every earlier incarnation of its transactional id.
     *
     * The single `InitProducerId` this sends is what makes a `transactional.id` more than a name: the coordinator
     * answers the producer id that `__transaction_state` holds for the id with an epoch **one higher** than the
     * previous incarnation used, which fences that incarnation for good, and it **aborts a transaction that
     * incarnation left open** before it answers - which is why the answer may be 51 (`ConcurrentTransactions`)
     * for a while and is simply retried here.
     *
     * It has to be called exactly once, before anything is sent, and it is an error to call it twice: the state
     * machine has no transition from `READY` back to `INITIALIZING`.
     *
     * @throws LogicException  For a producer without a `transactional.id`, or a second call
     * @throws KafkaException  For an error the coordinator reports and that a retry can not fix
     */
    public function initTransactions(): void
    {
        $this->ensureTransactional();
        $this->transitionTo(TransactionState::INITIALIZING);

        $this->producerIdAndEpoch = ProducerIdAndEpoch::none();
        $this->sequenceNumbers    = [];

        $transactionalId = $this->requireTransactionalId();

        try {
            $this->retrying(
                function (): void {
                    $this->transactionCoordinator = null;
                },
                function () use ($transactionalId): void {
                    $this->producerIdAndEpoch = $this->client->initProducerId(
                        $transactionalId,
                        $this->transactionTimeoutMs
                    );
                }
            );
        } catch (Throwable $error) {
            // A producer that never got its id can not do anything at all, so there is no state below fatal for it
            $this->transitionToFatalError($error);

            throw $error;
        }

        $this->transitionTo(TransactionState::READY);
    }

    /**
     * Opens a transaction.
     *
     * **There is no request behind this call**: the protocol has no "BeginTransaction" frame at all, and the
     * broker learns of the transaction with the first `AddPartitionsToTxn` of it, which is also what starts the
     * `transaction.timeout.ms` running. Until then this is nothing but a state change of the producer.
     *
     * @throws LogicException For a producer without a `transactional.id` or one that is not initialized
     * @throws Throwable      The error of a producer that is in an error state
     */
    public function beginTransaction(): void
    {
        $this->ensureTransactional();
        $this->maybeFailWithError();
        $this->transitionTo(TransactionState::IN_TRANSACTION);
    }

    /**
     * Enrols the topic-partitions of a batch into the open transaction, those that are not part of it yet.
     *
     * Every partition a transaction writes into has to be known to the coordinator **before** the first Produce
     * request that touches it, because the list of partitions is what the coordinator walks when it writes the
     * commit or abort markers; a partition that is missing from it would keep its records uncommitted for ever.
     * One call sends at most one `AddPartitionsToTxn` request, for every partition of the batch that the
     * coordinator does not hold yet.
     *
     * @param array<string, array<int, mixed>> $topicPartitions Whatever is keyed by topic and partition, e.g. the
     *        buffered records of a batch
     *
     * @throws LogicException For a producer that is not inside a transaction
     * @throws KafkaException For an error the coordinator reports
     */
    public function maybeAddPartitionsToTransaction(array $topicPartitions): void
    {
        $this->ensureTransactional();
        $this->failIfNotReadyForSend();

        foreach ($topicPartitions as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                $topicPartition = new TopicPartition((string) $topic, (int) $partition);
                if (!isset($this->partitionsInTransaction[(string) $topicPartition])) {
                    $this->newPartitionsInTransaction[(string) $topicPartition] = $topicPartition;
                }
            }
        }

        if ($this->newPartitionsInTransaction === []) {
            return;
        }

        $pending = $this->newPartitionsInTransaction;
        $request = [];
        foreach ($pending as $topicPartition) {
            $request[$topicPartition->topic][] = $topicPartition->partition;
        }

        $transactionalId = $this->requireTransactionalId();
        $this->transactionalRequest(fn(): mixed => $this->onTransactionCoordinator(
            function (Node $coordinator) use ($transactionalId, $request): void {
                $this->client->addPartitionsToTxn(
                    $coordinator,
                    $transactionalId,
                    $this->producerIdAndEpoch,
                    $request
                );
            }
        ));

        foreach ($pending as $key => $topicPartition) {
            $this->partitionsInTransaction[$key] = $topicPartition;
            unset($this->newPartitionsInTransaction[$key]);
        }
    }

    /**
     * Commits the offsets of a consumer group as part of the open transaction.
     *
     * This is the read side of the *consume-transform-produce* loop: the offsets the consumer would commit for
     * itself are committed by the **producer** instead, inside the transaction that holds the records it derived
     * from them, so that reading the input and writing the output either both happen or neither does. It takes two
     * requests - an `AddOffsetsToTxn` to the transaction coordinator, which puts the `__consumer_offsets`
     * partition of the group on the list of the transaction, and a `TxnOffsetCommit` to the **group** coordinator,
     * which writes the offsets as transactional records that stay invisible until the commit.
     *
     * The consumer of that group must **not** commit those offsets itself (`enable.auto.commit = false`), and it
     * has to read `read_committed`, otherwise it would see the records of a transaction that is later aborted.
     *
     * @param array<string, array<int, int|OffsetAndMetadata>> $topicPartitionOffsets Offsets to commit, as
     *        topic => partition => offset
     * @param string $groupId Consumer group the offsets belong to
     *
     * @throws LogicException For a producer that is not inside a transaction
     * @throws KafkaException For an error a coordinator reports
     */
    public function sendOffsetsToTransaction(array $topicPartitionOffsets, string $groupId): void
    {
        $this->ensureTransactional();
        $this->maybeFailWithError();

        if ($this->currentState !== TransactionState::IN_TRANSACTION) {
            throw new LogicException(
                'Can not send offsets to a transaction: the producer is in the state '
                . $this->currentState->value . ' instead of ' . TransactionState::IN_TRANSACTION->value
            );
        }
        if ($topicPartitionOffsets === []) {
            return;
        }

        $transactionalId = $this->requireTransactionalId();

        $this->transactionalRequest(fn(): mixed => $this->onTransactionCoordinator(
            function (Node $coordinator) use ($transactionalId, $groupId): void {
                $this->client->addOffsetsToTxn($coordinator, $transactionalId, $this->producerIdAndEpoch, $groupId);
            }
        ));
        $this->transactionalRequest(fn(): mixed => $this->onGroupCoordinator(
            $groupId,
            function (Node $coordinator) use ($transactionalId, $groupId, $topicPartitionOffsets): void {
                $this->client->txnOffsetCommit(
                    $coordinator,
                    $transactionalId,
                    $groupId,
                    $this->producerIdAndEpoch,
                    $topicPartitionOffsets
                );
            }
        ));
    }

    /**
     * Commits the open transaction: `EndTxn` with `transaction_result = COMMIT`.
     *
     * The coordinator answers as soon as it has written its decision into `__transaction_state`; the control
     * batches that make the records visible to a `read_committed` consumer are written afterwards, by the
     * coordinator itself, with a `WriteTxnMarkers` request per partition leader. A consumer therefore sees the
     * records a moment after this call returns, not while it runs.
     *
     * Everything the caller buffered has to be flushed **before** this call - a record that is still in the
     * producer's buffer when the transaction ends is not part of it.
     *
     * @throws LogicException For a producer that has no open transaction
     * @throws Throwable      The error of a producer that is in an error state
     * @throws KafkaException For an error the coordinator reports
     */
    public function commitTransaction(): void
    {
        $this->ensureTransactional();
        $this->maybeFailWithError();
        $this->transitionTo(TransactionState::COMMITTING_TRANSACTION);

        $this->endTransaction(EndTxnRequest::COMMIT);
    }

    /**
     * Aborts the open transaction: `EndTxn` with `transaction_result = ABORT`.
     *
     * This is the only way out of {@see TransactionState::ABORTABLE_ERROR}, and it is what a caller does when
     * anything of a transaction failed. The records that were already written stay in the log - they are never
     * deleted - but the coordinator marks them with an ABORT control batch, and a `read_committed` consumer drops
     * them; a `read_uncommitted` one has seen them all along.
     *
     * The partitions that were only *queued* for the transaction are dropped instead of being added first, exactly
     * as `TransactionManager.beginAbort()` @ 0.11.0.3 clears `newPartitionsInTransaction`: there is no point in
     * telling the coordinator about a partition that is about to be rolled back.
     *
     * @throws LogicException For a producer that has no open transaction
     * @throws KafkaException For an error the coordinator reports
     */
    public function abortTransaction(): void
    {
        $this->ensureTransactional();
        if ($this->currentState !== TransactionState::ABORTABLE_ERROR) {
            $this->maybeFailWithError();
        }
        $this->transitionTo(TransactionState::ABORTING_TRANSACTION);

        $this->newPartitionsInTransaction = [];

        $this->endTransaction(EndTxnRequest::ABORT);
    }

    /**
     * Refuses a `send()` that the state of this producer does not allow.
     *
     * `TransactionManager.failIfNotReadyForSend()` @ 0.11.0.3: a producer that hit an error may not send anything
     * else until the transaction is aborted, and a transactional producer may only send **inside** a transaction -
     * the batch of a transactional producer carries the transactional flag, and a broker answers a transactional
     * batch of a producer without an open transaction with the error code 48.
     *
     * @throws Throwable      The error of a producer that is in an error state
     * @throws LogicException For a transactional producer that is not initialized or has no open transaction
     */
    public function failIfNotReadyForSend(): void
    {
        $this->maybeFailWithError();

        if (!$this->isTransactional()) {
            return;
        }
        if (!$this->hasProducerId()) {
            throw new LogicException(
                'Can not send before initTransactions() was called: a transactional producer needs the producer id '
                . 'and the epoch that its coordinator hands out'
            );
        }
        if ($this->currentState !== TransactionState::IN_TRANSACTION) {
            throw new LogicException(
                'Can not send in the state ' . $this->currentState->value
                . ': a transactional producer writes inside a transaction, i.e. between beginTransaction() and '
                . 'commitTransaction() or abortTransaction()'
            );
        }
    }

    /**
     * Ends the open transaction and puts the producer back into `READY`
     */
    private function endTransaction(bool $transactionResult): void
    {
        $transactionalId = $this->requireTransactionalId();

        $this->transactionalRequest(fn(): mixed => $this->onTransactionCoordinator(
            function (Node $coordinator) use ($transactionalId, $transactionResult): void {
                $this->client->endTxn($coordinator, $transactionalId, $this->producerIdAndEpoch, $transactionResult);
            }
        ));

        $this->completeTransaction();
    }

    /**
     * Forgets everything that belonged to the transaction that just ended
     */
    private function completeTransaction(): void
    {
        $this->transitionTo(TransactionState::READY);

        $this->partitionsInTransaction    = [];
        $this->newPartitionsInTransaction = [];
    }

    /**
     * Moves the producer into another state, refusing a transition the state machine does not have.
     *
     * @throws LogicException For a transition that is not part of `TransactionManager.State` @ 0.11.0.3
     */
    private function transitionTo(TransactionState $target, ?Throwable $error = null): void
    {
        if (!$this->currentState->canTransitionTo($target)) {
            throw new LogicException(
                'Invalid transition attempted from the state ' . $this->currentState->value . ' to the state '
                . $target->value . ($this->transactionalId === null ? '' : " (transactionalId {$this->transactionalId})")
            );
        }

        if ($target === TransactionState::FATAL_ERROR || $target === TransactionState::ABORTABLE_ERROR) {
            if ($error === null) {
                throw new LogicException('Can not transition to ' . $target->value . ' without an error');
            }
            $this->lastError = $error;
            if ($target === TransactionState::FATAL_ERROR) {
                $this->fatalError = $error;
            }
        } else {
            $this->lastError = null;
        }

        $this->currentState = $target;
    }

    /**
     * Refuses a transactional call on a producer that has no `transactional.id`
     *
     * @throws LogicException
     */
    private function ensureTransactional(): void
    {
        if (!$this->isTransactional()) {
            throw new LogicException(
                'Transactional method invoked on a producer without a ' . 'transactional.id'
            );
        }
    }

    /**
     * Returns the `transactional.id` of this producer, for a call that only a transactional one may make
     *
     * @throws LogicException
     */
    private function requireTransactionalId(): string
    {
        $this->ensureTransactional();

        return (string) $this->transactionalId;
    }

    /**
     * Reports the error of a producer that is in one of the two error states
     *
     * @throws Throwable
     */
    private function maybeFailWithError(): void
    {
        if ($this->currentState->isError() && $this->lastError !== null) {
            throw $this->lastError;
        }
        $this->maybeThrowFatalError();
    }

    /**
     * Runs a call against the transaction coordinator of this producer, looking it up when it is not known
     */
    private function onTransactionCoordinator(Closure $call): void
    {
        $transactionalId = $this->requireTransactionalId();

        $this->retrying(
            function (): void {
                $this->transactionCoordinator = null;
            },
            function () use ($transactionalId, $call): void {
                $this->transactionCoordinator ??= $this->client->getTransactionCoordinator($transactionalId);

                $call($this->transactionCoordinator);
            }
        );
    }

    /**
     * Runs a call against the group coordinator of a consumer group, looking it up when it is not known
     */
    private function onGroupCoordinator(string $groupId, Closure $call): void
    {
        $this->retrying(
            function () use ($groupId): void {
                unset($this->groupCoordinators[$groupId]);
            },
            function () use ($groupId, $call): void {
                $this->groupCoordinators[$groupId] ??= $this->client->getGroupCoordinator($groupId);

                $call($this->groupCoordinators[$groupId]);
            }
        );
    }

    /**
     * Sends one request of the transaction protocol and puts the producer into the state its error implies.
     *
     * The classification is the one the handlers of `TransactionManager` @ 0.11.0.3 make. **Fatal** are the four
     * codes that say that this producer and its coordinator will never agree again - 47 `InvalidProducerEpoch`
     * (another incarnation of the transactional id took over), 48 `InvalidTxnState`, 49
     * `InvalidProducerIdMapping` and 53 `TransactionalIdAuthorizationFailed`. Everything else that reaches this
     * point - an authorization failure of a topic or a group, a request that timed out, a broken connection -
     * leaves the producer intact but the transaction unusable, so it becomes an **abortable** error and
     * {@see TransactionManager::abortTransaction()} is the only way on.
     *
     * @param Closure(): mixed $send The request to send
     *
     * @throws Throwable The error of the request, after the state was moved
     */
    private function transactionalRequest(Closure $send): void
    {
        try {
            $send();
        } catch (Throwable $error) {
            if ($error instanceof ProducerFencedException
                || $error instanceof InvalidTxnStateException
                || $error instanceof InvalidPidMappingException
                || $error instanceof TransactionalIdAuthorizationException
            ) {
                $this->transitionToFatalError($error);
            } elseif ($this->currentState->canTransitionTo(TransactionState::ABORTABLE_ERROR)) {
                $this->transitionToAbortableError($error);
            }

            throw $error;
        }
    }

    /**
     * Repeats a call of the transaction protocol for as long as the broker says "ask again".
     *
     * The three coordinator codes - 14 `GroupLoadInProgress`, 15 `GroupCoordinatorNotAvailable` and 16
     * `NotCoordinatorForGroup` - make the coordinator be looked up again, and **51** `ConcurrentTransactions`, the
     * answer of a coordinator that is still completing the previous transaction of this id, is retried against the
     * same coordinator. Everything else is the caller's business, including the codes that end the producer.
     *
     * **The budget of these retries is a deadline, not the `retries` of a batch.** None of the four codes says
     * that the request failed - they say that the coordinator is not ready to answer it yet, exactly as the three
     * of a {@see \Protocol\Kafka\Common\CoordinatorLookup} do - and the most common one of them, 51, is the answer
     * of a coordinator that is *aborting the transaction the previous incarnation of this transactional id left
     * open*, which takes as long as writing a marker into every partition of it. A producer with `retries = 0`
     * would give up on that after a single attempt. The deadline is therefore
     * `metadata.fetch.timeout.ms`, the one the coordinator lookup itself uses, with `retry.backoff.ms` between the
     * attempts.
     *
     * @param Closure(): void $forgetCoordinator Drops the cached coordinator, so the next attempt looks it up
     * @param Closure(): void $call              The call to repeat
     *
     * @throws KafkaException The error of the last attempt
     */
    private function retrying(Closure $forgetCoordinator, Closure $call): void
    {
        $policy    = RetryPolicy::fromConfiguration($this->configuration);
        $timeoutMs = (int) ($this->configuration[ClientConfig::METADATA_FETCH_TIMEOUT_MS]
            ?? self::DEFAULT_COORDINATOR_TIMEOUT_MS);
        $deadline  = microtime(true) + $timeoutMs / 1000;

        for ($attempt = 1;; $attempt++) {
            try {
                $call();

                return;
            } catch (GroupCoordinatorNotAvailableException | NotCoordinatorForGroupException $error) {
                $forgetCoordinator();
            } catch (GroupLoadInProgressException | ConcurrentTransactionsException $error) {
                // The coordinator is the right one, it is only busy with the transaction before this one
            }

            if ($attempt >= $policy->getMaxAttempts() && microtime(true) >= $deadline) {
                throw $error;
            }
            $policy->backoff();
        }
    }
}
