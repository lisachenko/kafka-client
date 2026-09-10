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
 * One replica of a DescribeLogDirs answer, i.e. one entry of the `partitions` array of a topic of a log directory
 *
 * <pre>
 *   DescribeLogDirsResponsePartition => partition size offset_lag is_future
 *     partition  => INT32
 *     size       => INT64
 *     offset_lag => INT64
 *     is_future  => BOOLEAN
 * </pre>
 *
 * `DESCRIBE_LOG_DIRS_RESPONSE_V0` in `DescribeLogDirsResponse.schemaVersions()` @ 1.1.1, built by
 * `ReplicaManager.describeLogDirs` as `new ReplicaInfo(log.size, getLogEndOffsetLag(…), log.isFuture)`:
 *
 *  - `size` is the number of bytes the **log segments** of this replica occupy in this directory, indexes excluded
 *    (`Log.size` sums the `FileRecords` of the segments), so it grows in whole batches and is 0 for a future log
 *    that has not copied anything yet;
 *  - `offset_lag` is how far this log is behind: for the **current** log `max(highWatermark - logEndOffset, 0)`,
 *    which is 0 on a one-broker cluster, and for a **future** log `logEndOffset(current) - logEndOffset(future)`,
 *    i.e. the number of records the mover still has to copy. A replica that the broker does not have at all is
 *    reported with {@see self::INVALID_OFFSET_LAG} (-1);
 *  - `is_future` marks the log that an AlterReplicaLogDirs request created and that
 *    `ReplicaAlterLogDirsThread` is filling; it disappears the moment the mover swaps it in as the current log.
 *
 * @see docs/protocol/1.1.md, section "DescribeLogDirs API (key 35, v0)"
 */
class DescribeLogDirsResponsePartition implements BinarySchemaInterface
{
    /**
     * The lag of a replica the broker does not have, `DescribeLogDirsResponse.INVALID_OFFSET_LAG`
     */
    public const int INVALID_OFFSET_LAG = -1;

    /**
     * Id of the partition this entry belongs to
     */
    public int $partition;

    /**
     * Size of the log segments of this replica in this directory, in bytes
     */
    public int $size;

    /**
     * Number of records this log is behind the log it follows, or {@see self::INVALID_OFFSET_LAG}
     */
    public int $offsetLag;

    /**
     * Whether this log is the future log of a running replica move and not the log the partition is served from
     */
    public bool $isFuture;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'size'      => BinarySchema::TYPE_INT64,
            'offsetLag' => BinarySchema::TYPE_INT64,
            'isFuture'  => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}
