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
 * One partition of an OffsetForLeaderEpoch request, version 0 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochRequestPartition => partition_id leader_epoch
 *     partition_id => INT32
 *     leader_epoch => INT32
 * </pre>
 *
 * The property is called `partition` here, as in every other partition DTO of this client; the wire name of the
 * field is `partition_id` (`OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0` in `Protocol.java` @ 0.11.0.3).
 *
 * `leader_epoch` is the epoch the asking replica believes it followed, i.e. the value that the record batches it
 * has on disk carry in `partition_leader_epoch` ({@see \Protocol\Kafka\Common\Record\RecordBatch::$partitionLeaderEpoch}).
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0)"
 */
class OffsetForLeaderEpochRequestPartition implements BinarySchemaInterface
{
    /**
     * Id of the partition to ask the end offset of an epoch for
     */
    public int $partition;

    /**
     * Epoch whose end offset is asked for
     */
    public int $leaderEpoch;

    public function __construct(int $partition, int $leaderEpoch)
    {
        $this->partition   = $partition;
        $this->leaderEpoch = $leaderEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'   => BinarySchema::TYPE_INT32,
            'leaderEpoch' => BinarySchema::TYPE_INT32,
        ];
    }
}
