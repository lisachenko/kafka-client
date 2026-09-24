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

use Protocol\Kafka\Common\Errors\KafkaException;

/**
 * Observer of the acknowledgements a share consumer sent, once the node answered them (KIP-932)
 *
 * `AcknowledgementCommitCallback` of the Java client @ 4.3.1, set with
 * {@see KafkaShareConsumer::setAcknowledgementCommitCallback()}. It is called once per topic-partition of every
 * request that carried acknowledgements - the ShareFetch of a {@see KafkaShareConsumer::poll()}, the ShareAcknowledge
 * of {@see KafkaShareConsumer::commitSync()} and {@see KafkaShareConsumer::commitAsync()}, and the one that closes a
 * share session in {@see KafkaShareConsumer::close()} - with the offsets of that partition and the error the node
 * answered for them, null when they were acknowledged.
 *
 * PHP has no background thread, so the callback runs inside the call that sent the acknowledgements, on the thread
 * of the caller, and the methods of the consumer are not accessible from it, as in the Java client: a callback that
 * calls one gets a {@see \LogicException}. An exception the callback throws itself is swallowed, as the Java handler
 * logs and swallows it.
 *
 * The errors a callback sees are the ones of the acknowledgement:
 * - {@see \Protocol\Kafka\Common\Errors\InvalidRecordStateException} (121) for an offset this member does not hold
 *   any more - its acquisition lock expired, or another member acquired it since;
 * - {@see \Protocol\Kafka\Common\Errors\ShareSessionNotFoundException} (122) and
 *   {@see \Protocol\Kafka\Common\Errors\InvalidShareSessionEpochException} (123) when the share session the records
 *   were acquired in was lost - a connection that dropped takes the session with it, and the records are delivered
 *   again;
 * - {@see \Protocol\Kafka\Common\Errors\NotLeaderForPartitionException} (6) when the leader of the partition moved.
 *
 * Even a retriable error means that the acknowledgement did not complete: the records have to be fetched again.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
interface AcknowledgementCommitCallback
{
    /**
     * Called when an acknowledgement request sent to the node has been completed
     *
     * @param array<string, array<int, list<int>>> $offsets   [topic][partition] => the acknowledged offsets, one
     *                                                        topic-partition per call
     * @param KafkaException|null                  $exception Error of the acknowledgement, null when it succeeded
     */
    public function onComplete(array $offsets, ?KafkaException $exception): void;
}
