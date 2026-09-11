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

use Protocol\Kafka\Common\Record\RecordBatch;
use Stringable;

/**
 * The two values that `InitProducerId` hands out and that every record batch of an idempotent producer carries.
 *
 * They belong together: the broker keeps its sequence numbers per producer **id**, and the **epoch** is what tells
 * two producers of the same id apart, so a batch that carries one of them without the other is meaningless. The
 * pair is immutable - a producer that has to start over gets a new one, it does not change the one it holds - and
 * {@see ProducerIdAndEpoch::none()} is the pair of a producer that has no producer id at all, the -1 / -1 that a
 * plain producer writes into its batches.
 *
 * @see \Protocol\Kafka\Producer\Internals\TransactionManager
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0)"
 */
final class ProducerIdAndEpoch implements Stringable
{
    public function __construct(
        /**
         * Producer id that the broker handed out, {@see RecordBatch::NO_PRODUCER_ID} without one
         */
        public readonly int $producerId = RecordBatch::NO_PRODUCER_ID,
        /**
         * Epoch of that producer id, {@see RecordBatch::NO_PRODUCER_EPOCH} without one
         */
        public readonly int $epoch = RecordBatch::NO_PRODUCER_EPOCH
    ) {}

    /**
     * The pair of a producer that has no producer id, `ProducerIdAndEpoch.NONE` of the Java client
     */
    public static function none(): self
    {
        return new self(RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH);
    }

    /**
     * Tells whether these two values name a real producer, i.e. whether the id was handed out by a broker
     */
    public function isValid(): bool
    {
        return $this->producerId > RecordBatch::NO_PRODUCER_ID;
    }

    /**
     * Tells whether this is the very pair that a batch was written with
     */
    public function matches(int $producerId, int $epoch): bool
    {
        return $this->producerId === $producerId && $this->epoch === $epoch;
    }

    public function __toString(): string
    {
        return "(producerId={$this->producerId}, epoch={$this->epoch})";
    }
}
