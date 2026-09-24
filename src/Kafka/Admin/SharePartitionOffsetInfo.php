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

use Protocol\Kafka\Protocol\Data\DescribeShareGroupOffsetsResponsePartition;

/**
 * Where a share group stands in one partition, as {@see AdminClient::listShareGroupOffsets()} reports it
 *
 * `SharePartitionOffsetInfo` of the Java admin client @ 4.3.1. A share group commits no offset: its members
 * acknowledge records one by one, and the share coordinator keeps the **start offset** of every share partition, the
 * earliest offset of the records the group is not done with yet - some records after it may already be done. The
 * **lag** of KIP-1226 (DescribeShareGroupOffsets v1, Kafka 4.2) is what the group still has to deliver from that
 * start offset to the end of the partition: `end offset - start offset - the records past the start offset that are
 * done with`.
 *
 * The leader epoch and the lag are `null` where the Java class has an empty `Optional`: when the node answered a
 * negative value - the -1 of a lag that is not known yet, which is every lag until the group acknowledged something
 * ({@see DescribeShareGroupOffsetsResponsePartition::UNINITIALIZED_LAG}).
 *
 * @see docs/protocol/4.3.md, section "The share-group admin methods"
 */
final class SharePartitionOffsetInfo
{
    public function __construct(
        /**
         * The share-partition start offset: the earliest offset of the records the group is not done with
         */
        public readonly int $startOffset,
        /**
         * The leader epoch of the partition the state was written at, null when the node answered none
         */
        public readonly ?int $leaderEpoch,
        /**
         * The records of the partition the group still has to deliver (KIP-1226), null when it is not known
         */
        public readonly ?int $lag
    ) {}

    /**
     * Builds the info of one partition of a DescribeShareGroupOffsets answer, `null` for a partition without a
     * start offset - the -1 of a group that holds no state of it, as the Java `ListShareGroupOffsetsHandler` maps it
     */
    public static function fromResponsePartition(DescribeShareGroupOffsetsResponsePartition $partition): ?self
    {
        if ($partition->startOffset < 0) {
            return null;
        }

        return new self(
            $partition->startOffset,
            $partition->leaderEpoch < 0 ? null : $partition->leaderEpoch,
            $partition->lag < 0 ? null : $partition->lag
        );
    }
}
