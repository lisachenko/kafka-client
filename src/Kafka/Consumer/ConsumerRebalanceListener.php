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

namespace Protocol\Kafka\Consumer;

/**
 * Observer of the partitions that a rebalance takes away from a consumer and hands to it.
 *
 * A group is rebalanced whenever its membership changes - another consumer subscribed to the same group, one of
 * them left or missed its `session.timeout.ms` - and a consumer notices that inside {@see KafkaConsumer::poll()}.
 * The two callbacks bracket that moment: everything the application still has to do with the partitions it is
 * losing belongs into {@see onPartitionsRevoked()} - committing where it stopped, flushing a buffer, releasing a
 * lock - and {@see onPartitionsAssigned()} is called once the new assignment is in place and its positions have
 * been read from the committed offsets of the group.
 *
 * With `enable.auto.commit` on, the consumer commits its positions before it calls {@see onPartitionsRevoked()},
 * so a listener is only needed to commit when the automatic commit is switched off.
 *
 * Both callbacks are executed inside poll(), on the thread of the caller: a listener that throws aborts the
 * rebalance and the exception reaches the application, and a listener that blocks for longer than
 * `session.timeout.ms` costs this consumer its membership.
 *
 * @see \Protocol\Kafka\Consumer\KafkaConsumer::subscribe()
 * @see docs/protocol/0.11.0.md, section "Group membership protocol (keys 11 to 14)"
 */
interface ConsumerRebalanceListener
{
    /**
     * Called before a rebalance takes the current assignment of the consumer away from it
     *
     * @param array<string, list<int>> $partitions Partitions the consumer is losing, by topic; the assignment it
     *                                             held until this moment
     */
    public function onPartitionsRevoked(array $partitions): void;

    /**
     * Called after a rebalance handed a new assignment to the consumer
     *
     * @param array<string, list<int>> $partitions Partitions the consumer received, by topic; empty when the group
     *                                             has more members than the subscribed topics have partitions
     */
    public function onPartitionsAssigned(array $partitions): void;
}
