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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One partition of an OffsetForLeaderEpoch request, version 2 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochRequestPartition => partition_id current_leader_epoch leader_epoch
 *     partition_id         => INT32
 *     current_leader_epoch => INT32     -- since version 2
 *     leader_epoch         => INT32
 * </pre>
 *
 * **Version 2 (Kafka 2.1, KIP-320) inserted `current_leader_epoch`** between the partition index and the epoch to
 * look up, see {@see self::$currentLeaderEpoch}. The two fields are not the same question: `leader_epoch` is the
 * epoch whose end offset is asked for, `current_leader_epoch` is the epoch the asking client believes the
 * partition is being led with **now**, and it only fences the request.
 *
 * The property is called `partition` here, as in every other partition DTO of this client; the wire name of the
 * field is `partition_id` (`OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0` in `Protocol.java` @ 0.11.0.3).
 *
 * `leader_epoch` is the epoch the asking replica believes it followed, i.e. the value that the record batches it
 * has on disk carry in `partition_leader_epoch` ({@see \Protocol\Kafka\Common\Record\RecordBatch::$partitionLeaderEpoch}).
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v4)"
 */
class OffsetForLeaderEpochRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the OffsetForLeaderEpoch API that this DTO is packed for
     */
    public const int VERSION = 2;

    /**
     * Value of `current_leader_epoch` for a client that does not fence its request on the leadership
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * Id of the partition to ask the end offset of an epoch for
     */
    public int $partition;

    /**
     * Epoch the client believes this partition is being led with, the field version 2 added (Kafka 2.1, KIP-320)
     *
     * It fences the request exactly as the field of the same name fences a Fetch v9: an epoch older than the one
     * the leader is on is **74** `FENCED_LEADER_EPOCH`, a newer one **75** `UNKNOWN_LEADER_EPOCH`.
     * {@see self::UNKNOWN_LEADER_EPOCH} switches the check off, and it is what a request below version 2 is
     * served as, because it does not carry the field at all.
     *
     * @since Version 2 of protocol
     */
    public int $currentLeaderEpoch = self::UNKNOWN_LEADER_EPOCH;

    /**
     * Epoch whose end offset is asked for
     */
    public int $leaderEpoch;

    public function __construct(
        int $partition,
        int $leaderEpoch,
        int $currentLeaderEpoch = self::UNKNOWN_LEADER_EPOCH
    ) {
        $this->partition          = $partition;
        $this->leaderEpoch        = $leaderEpoch;
        $this->currentLeaderEpoch = $currentLeaderEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = ['partition' => BinarySchema::TYPE_INT32];
        if (static::VERSION >= 2) {
            $scheme['currentLeaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        $scheme['leaderEpoch'] = BinarySchema::TYPE_INT32;

        return $scheme;
    }
}
