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

use Protocol\Kafka\Protocol\Data\DescribeLogDirsResponsePartition;

/**
 * What one replica occupies in one log directory of one broker
 *
 * `org.apache.kafka.common.requests.DescribeLogDirsResponse.ReplicaInfo` of the Java client, with its three fields:
 *
 *  - `size` - the bytes the log segments of this replica occupy in this directory;
 *  - `offsetLag` - how many records this log is behind the log it follows. For the **current** log of a partition
 *    that is `max(highWatermark - logEndOffset, 0)`, which is 0 on a healthy leader; for a **future** log it is the
 *    number of records the mover still has to copy, and it falls to 0 just before the swap;
 *  - `isFuture` - whether this is the log an {@see AdminClient::alterReplicaLogDirs()} created and that will
 *    replace the current one, rather than the log the partition is served from right now.
 *
 * A replica whose lag the broker cannot tell - because it has no replica object for the partition at all - carries
 * {@see self::INVALID_OFFSET_LAG}.
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0 and v1)"
 */
final class ReplicaInfo
{
    /**
     * The lag of a replica the broker does not have, `DescribeLogDirsResponse.INVALID_OFFSET_LAG`
     */
    public const int INVALID_OFFSET_LAG = DescribeLogDirsResponsePartition::INVALID_OFFSET_LAG;

    /**
     * @param int  $size      Bytes the log segments of this replica occupy in this directory
     * @param int  $offsetLag Records this log is behind the log it follows, or {@see self::INVALID_OFFSET_LAG}
     * @param bool $isFuture  Whether this is the future log of a running move
     */
    public function __construct(
        public readonly int $size,
        public readonly int $offsetLag,
        public readonly bool $isFuture,
    ) {}

    /**
     * Builds the result from the partition entry of a DescribeLogDirs answer
     */
    public static function fromResponsePartition(DescribeLogDirsResponsePartition $partition): self
    {
        return new self($partition->size, $partition->offsetLag, $partition->isFuture);
    }
}
