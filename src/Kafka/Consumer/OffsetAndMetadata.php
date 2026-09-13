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
 * The metadata is opaque to the broker: it stores the string and hands it back with the next OffsetFetch. A broker
 * rejects a commit whose metadata is longer than `offset.metadata.max.bytes` (4096 by default) with the error code
 * 12, OffsetMetadataTooLarge.
 *
 * **The leader epoch arrived with Kafka 2.1** (KIP-320): version 6 of the OffsetCommit api and version 2 of the TxnOffsetCommit api carry a
 * `committed_leader_epoch` per partition and version 5 of the OffsetFetch api hands it back, so that a consumer
 * that resumes from a committed offset can tell the broker which leader that offset was read from. A partition
 * whose epoch is unknown - a client that never fetched it, an offset committed by a client of an older release, or
 * a broker below 2.1 - carries {@see self::UNKNOWN_LEADER_EPOCH} on the wire, and `null` here, exactly as the Java
 * `OffsetAndMetadata.leaderEpoch()` carries an empty `Optional`.
 *
 * @see docs/protocol/2.8.md, section "The leader epoch of a committed offset (KIP-320)"
 */
final class OffsetAndMetadata implements \Stringable
{
    /**
     * The value the wire uses for "the leader epoch of this offset is not known", `RecordBatch
     * .NO_PARTITION_LEADER_EPOCH` in the Java client
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * @param int         $offset      The offset to commit for a topic-partition
     * @param string|null $metadata    Any associated metadata the client wants the broker to keep, or null for none
     * @param int|null    $leaderEpoch Epoch of the leader this offset was read from, null when it is not known
     *        (`committed_leader_epoch` of OffsetCommit v6 and OffsetFetch v5, Kafka 2.1, KIP-320)
     */
    public function __construct(
        public readonly int $offset,
        public readonly ?string $metadata = null,
        public readonly ?int $leaderEpoch = null
    ) {}

    public function __toString(): string
    {
        $parts = ["offset={$this->offset}"];
        if ($this->metadata !== null) {
            $parts[] = "metadata='{$this->metadata}'";
        }
        if ($this->leaderEpoch !== null) {
            $parts[] = "leaderEpoch={$this->leaderEpoch}";
        }

        return 'OffsetAndMetadata{' . implode(', ', $parts) . '}';
    }
}
