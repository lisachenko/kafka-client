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
 * The committed position of a topic-partition, together with the metadata the client keeps next to it.
 *
 * The metadata is opaque to the broker: it stores the string and hands it back with the next OffsetFetch. Kafka
 * 0.10.2.2 rejects a commit whose metadata is longer than `offset.metadata.max.bytes` (4096 by default) with the
 * error code 12, OffsetMetadataTooLarge.
 *
 * Kafka 2.1 added the **leader epoch** of the record the offset points behind (KIP-320): the epoch travels with a
 * commit from `TxnOffsetCommit` **v2** and from `OffsetCommit` v6 on, and a broker uses it to refuse a commit that
 * a fenced leader produced. {@see self::NO_LEADER_EPOCH} - the -1 both apis default to - means "the client does
 * not know it", which is what every offset of this client carries until the consumer tracks epochs (KIP-320 in the
 * consumer is a later minor of this line).
 */
final class OffsetAndMetadata implements \Stringable
{
    /**
     * The leader epoch of an offset whose epoch the client does not know, the default of every api that carries one
     *
     * `RecordBatch.NO_PARTITION_LEADER_EPOCH` of the Java client, and the `default: -1` of `committed_leader_epoch`
     * in `TxnOffsetCommitRequest.json` and `OffsetCommitRequest.json` @ 2.8.2.
     */
    public const int NO_LEADER_EPOCH = -1;

    /**
     * @param int         $offset      The offset to commit for a topic-partition
     * @param string|null $metadata    Any associated metadata the client wants the broker to keep, or null for none
     * @param int         $leaderEpoch Leader epoch of the record the offset points behind (KIP-320), or
     *                                 {@see self::NO_LEADER_EPOCH} when the client does not know it
     */
    public function __construct(
        public readonly int $offset,
        public readonly ?string $metadata = null,
        public readonly int $leaderEpoch = self::NO_LEADER_EPOCH
    ) {}

    public function __toString(): string
    {
        return $this->metadata === null
            ? "OffsetAndMetadata{offset={$this->offset}}"
            : "OffsetAndMetadata{offset={$this->offset}, metadata='{$this->metadata}'}";
    }
}
