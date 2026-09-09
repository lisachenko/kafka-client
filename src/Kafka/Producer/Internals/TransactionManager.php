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

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Errors\DuplicateSequenceNumberException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\OutOfOrderSequenceException;
use Protocol\Kafka\Common\Errors\ProducerFencedException;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
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
 * sequence numbers. The transactional half - the state machine of `beginTransaction`, `AddPartitionsToTxn`,
 * `AddOffsetsToTxn`, `TxnOffsetCommit` and `EndTxn` - lives on top of exactly these members and is added by the
 * ticket that implements it; {@see TransactionManager::isTransactional()} already tells the two apart, and a
 * manager that carries a transactional id already asks its **transaction coordinator** for the producer id.
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
 * @see docs/protocol/0.11.0.md, sections "InitProducerId API (key 22, v0)" and "The idempotent producer"
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
     * @param Client      $client               Client that sends the `InitProducerId` request
     * @param string|null $transactionalId      `transactional.id` of the producer, `null` for a merely idempotent
     *        one; a non-null id makes the producer id survive a restart and is looked up at the transaction
     *        coordinator of that id
     * @param int         $transactionTimeoutMs `transaction.timeout.ms`, only meaningful with a transactional id
     */
    public function __construct(
        private readonly Client $client,
        private readonly ?string $transactionalId = null,
        private readonly int $transactionTimeoutMs = InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS
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
     * state and is left to the caller.
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
    }

    /**
     * Puts the producer into the state it can not leave: every following call reports this error.
     */
    public function transitionToFatalError(Throwable $exception): void
    {
        $this->fatalError = $exception;
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
}
