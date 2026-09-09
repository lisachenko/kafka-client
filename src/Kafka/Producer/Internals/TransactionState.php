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

/**
 * The states a transactional producer moves through, `TransactionManager.State` of the Java client @ 0.11.0.3.
 *
 * The state machine is entirely **client-side**: the broker knows the states of `__transaction_state` (`Empty`,
 * `Ongoing`, `PrepareCommit`, `PrepareAbort`, `CompleteCommit`, `CompleteAbort`, `Dead`), and these are the ones
 * the *producer* keeps so that it can refuse a call that the protocol has no request for - there is no
 * "BeginTransaction" frame, and `send()` inside a transaction is an ordinary Produce request that only differs by
 * the transactional flag of its batch.
 *
 * ```
 *   UNINITIALIZED ──initTransactions()──▶ INITIALIZING ──InitProducerId──▶ READY
 *                                                                          │  ▲
 *                                                    beginTransaction() ───┘  │
 *                                                                             │
 *   IN_TRANSACTION ──commitTransaction()──▶ COMMITTING ──EndTxn(commit)───────┤
 *        │         ──abortTransaction()───▶ ABORTING   ──EndTxn(abort)────────┘
 *        │
 *        └──an error that a rollback can undo──▶ ABORTABLE_ERROR ──abortTransaction()──▶ ABORTING
 *
 *   any state ──a fencing or a protocol error──▶ FATAL_ERROR (final)
 * ```
 *
 * The only way out of {@see self::ABORTABLE_ERROR} is {@see \Protocol\Kafka\Producer\KafkaProducer::abortTransaction()},
 * and there is no way out of {@see self::FATAL_ERROR} at all: a producer that was fenced by a newer incarnation of
 * its transactional id can never write again, whatever it does, so every following call reports the same error.
 *
 * @see docs/protocol/0.11.0.md, section "Transactions"
 */
enum TransactionState: string
{
    /**
     * No producer id yet: `initTransactions()` was not called
     */
    case UNINITIALIZED = 'UNINITIALIZED';

    /**
     * `initTransactions()` is in flight, the `InitProducerId` was not answered yet
     */
    case INITIALIZING = 'INITIALIZING';

    /**
     * A producer id and an epoch are known, and no transaction is open
     */
    case READY = 'READY';

    /**
     * A transaction is open: records may be sent and offsets may be added to it
     */
    case IN_TRANSACTION = 'IN_TRANSACTION';

    /**
     * `commitTransaction()` is in flight
     */
    case COMMITTING_TRANSACTION = 'COMMITTING_TRANSACTION';

    /**
     * `abortTransaction()` is in flight
     */
    case ABORTING_TRANSACTION = 'ABORTING_TRANSACTION';

    /**
     * Something went wrong that only an abort can clean up; every call but `abortTransaction()` reports the error
     */
    case ABORTABLE_ERROR = 'ABORTABLE_ERROR';

    /**
     * The producer is finished for good and reports the same error for ever
     */
    case FATAL_ERROR = 'FATAL_ERROR';

    /**
     * Tells whether the producer may move from this state into the given one.
     *
     * This is `State.isTransitionValid()` of the Java client, byte for byte: {@see self::FATAL_ERROR} is reachable
     * from everywhere and reaches nothing, {@see self::ABORTING_TRANSACTION} is reachable from an abortable error
     * as well as from an open transaction, and {@see self::READY} is what both ways of completing a transaction -
     * and the initialization - lead back to.
     */
    public function canTransitionTo(self $target): bool
    {
        return match ($target) {
            self::INITIALIZING           => $this === self::UNINITIALIZED,
            self::READY                  => in_array(
                $this,
                [self::INITIALIZING, self::COMMITTING_TRANSACTION, self::ABORTING_TRANSACTION],
                true
            ),
            self::IN_TRANSACTION         => $this === self::READY,
            self::COMMITTING_TRANSACTION => $this === self::IN_TRANSACTION,
            self::ABORTING_TRANSACTION   => $this === self::IN_TRANSACTION || $this === self::ABORTABLE_ERROR,
            self::ABORTABLE_ERROR        => in_array(
                $this,
                [self::IN_TRANSACTION, self::COMMITTING_TRANSACTION, self::ABORTABLE_ERROR],
                true
            ),
            // "We can transition to FATAL_ERROR unconditionally", TransactionManager.java @ 0.11.0.3
            self::FATAL_ERROR            => true,
            self::UNINITIALIZED          => false,
        };
    }

    /**
     * Tells whether this state is one of the two error states, which every transactional call but an abort refuses
     */
    public function isError(): bool
    {
        return $this === self::ABORTABLE_ERROR || $this === self::FATAL_ERROR;
    }
}
