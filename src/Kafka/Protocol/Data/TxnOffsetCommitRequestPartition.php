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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One partition of a TxnOffsetCommit request, i.e. one entry of the `partitions` array of a topic
 *
 * <pre>
 *   TxnOffsetCommitRequestPartition (Version: 2) => partition offset committed_leader_epoch metadata
 *     partition             => INT32
 *     offset                => INT64
 *     committed_leader_epoch => INT32     -- since version 2
 *     metadata              => NULLABLE_STRING
 * </pre>
 *
 * `TXN_OFFSET_COMMIT_PARTITION_OFFSET_METADATA_REQUEST_V0` in `Protocol.java` @ 0.11.0.3. It is the entry of an
 * {@see OffsetCommitRequestPartition} **without the timestamp of the v1 request**: a transactional commit carries
 * the offset to store and the free-form metadata the consumer wants to keep with it, and nothing else. The
 * retention of the commit is the one the group coordinator applies by itself, there is no `retention_time` in this
 * api.
 *
 * **Kafka 2.1 added the `committed_leader_epoch` of version 2** (KIP-320), between the offset and the metadata -
 * `{ "name": "CommittedLeaderEpoch", "type": "int32", "versions": "2+", "default": "-1" }` in
 * `TxnOffsetCommitRequest.json` @ 2.8.2. It is the epoch of the leader that produced the record the offset points
 * behind, and the coordinator stores it with the offset so that a later fetch can tell a truncated log from a
 * current one. A client that does not track epochs sends {@see OffsetAndMetadata::NO_LEADER_EPOCH} (-1), which is
 * what this client does until the consumer of a later minor learns them; {@see TxnOffsetCommitRequestPartitionV0}
 * is the entry of the versions 0 and 1, which have no such field.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v2)"
 */
class TxnOffsetCommitRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the TxnOffsetCommit API that this DTO belongs to
     */
    public const int VERSION = 2;

    /**
     * Id of the partition whose offset is committed
     */
    public int $partition;

    /**
     * Offset of the next record the group will read from that partition
     */
    public int $offset;

    /**
     * Leader epoch of the record the offset points behind, -1 when the client does not know it
     *
     * @since Version 2 of protocol
     */
    public int $committedLeaderEpoch = OffsetAndMetadata::NO_LEADER_EPOCH;

    /**
     * Free-form metadata the consumer keeps next to the offset, `null` for none
     */
    public ?string $metadata;

    public function __construct(
        int $partition,
        int|OffsetAndMetadata $offset,
        ?string $metadata = null,
        int $leaderEpoch = OffsetAndMetadata::NO_LEADER_EPOCH
    ) {
        $this->partition            = $partition;
        $this->offset               = $offset instanceof OffsetAndMetadata ? $offset->offset : $offset;
        $this->metadata             = $offset instanceof OffsetAndMetadata ? $offset->metadata : $metadata;
        $this->committedLeaderEpoch = $offset instanceof OffsetAndMetadata ? $offset->leaderEpoch : $leaderEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition' => BinarySchema::TYPE_INT32,
            'offset'    => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 2) {
            $scheme['committedLeaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        $scheme['metadata'] = BinarySchema::TYPE_NULLABLE_STRING;

        return $scheme;
    }
}
