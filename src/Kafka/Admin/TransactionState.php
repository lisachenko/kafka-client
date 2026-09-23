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

namespace Protocol\Kafka\Admin;

/**
 * The state a transaction coordinator holds a transactional id in (`TransactionState` of the Java admin client)
 *
 * The values are the names that travel on the wire in the `transaction_state` field of a DescribeTransactions
 * (key 65) and a ListTransactions (key 66) answer, and they are the `TransactionState.name` constants of
 * `kafka.coordinator.transaction.TransactionMetadata` @ 3.9.2. The same names are what the `state_filters` of a
 * ListTransactions request are matched against, verbatim: `TransactionState.fromName` @ 3.9.2 looks the string up
 * in a map of exactly these names, and anything else comes back in the `unknown_state_filters` of the answer.
 *
 * The life of a transaction runs `Empty` → `Ongoing` (the first partition was added) → `PrepareCommit` or
 * `PrepareAbort` (the coordinator wrote its decision) → `CompleteCommit` or `CompleteAbort` (every marker was
 * written), and back to `Ongoing` for the next transaction of the same id. {@see self::PrepareEpochFence} is the
 * state of an id whose epoch is being bumped by an `InitProducerId` of a new producer instance (KIP-360), and
 * {@see self::Dead} the transient state of an id whose metadata is being removed - the coordinator never reports
 * it, because a describe of such an id answers 105 and a listing filters it out.
 *
 * A name this client does not know folds into {@see self::Unknown}, exactly as `TransactionState.parse` of the
 * Java admin client does with its `UNKNOWN`: a broker of a later release may hold a state this line has no case
 * for, and that must not be an exception.
 *
 * @see docs/protocol/4.3.md, sections "DescribeTransactions API (key 65, v0)" and "ListTransactions API (key 66, v0 and v1)"
 */
enum TransactionState: string
{
    /**
     * The id is known to the coordinator and has no transaction in flight - the state after `InitProducerId`
     */
    case Empty = 'Empty';

    /**
     * A transaction of this id is open and at least one partition has been added to it
     */
    case Ongoing = 'Ongoing';

    /**
     * The coordinator has decided to commit and is writing the markers of the transaction
     */
    case PrepareCommit = 'PrepareCommit';

    /**
     * The coordinator has decided to abort and is writing the markers of the transaction
     */
    case PrepareAbort = 'PrepareAbort';

    /**
     * Every commit marker has been written; the id waits for the next transaction
     */
    case CompleteCommit = 'CompleteCommit';

    /**
     * Every abort marker has been written; the id waits for the next transaction
     */
    case CompleteAbort = 'CompleteAbort';

    /**
     * The metadata of this id is being expired and removed - never reported to a client
     */
    case Dead = 'Dead';

    /**
     * The epoch of this id is being bumped, which fences the producer that held the previous one (KIP-360)
     */
    case PrepareEpochFence = 'PrepareEpochFence';

    /**
     * A state name this client does not know, which is never sent by a 3.9.2 node
     */
    case Unknown = 'Unknown';

    /**
     * Turns the `transaction_state` of an answer into one of these cases, folding an unknown name into `Unknown`
     */
    public static function fromWire(string $name): self
    {
        return self::tryFrom($name) ?? self::Unknown;
    }

    /**
     * Returns whether a transaction of this id is in flight, i.e. its markers have not all been written yet
     */
    public function isInFlight(): bool
    {
        return match ($this) {
            self::Ongoing, self::PrepareCommit, self::PrepareAbort => true,
            default                                               => false,
        };
    }
}
