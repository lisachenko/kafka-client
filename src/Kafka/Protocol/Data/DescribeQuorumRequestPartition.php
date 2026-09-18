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
 * One partition a DescribeQuorum request asks about (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   PartitionData => PartitionIndex
 *     PartitionIndex => INT32
 * </pre>
 *
 * The api is about the raft quorum of a partition, and the only partition a KRaft cluster replicates that way is
 * the partition 0 of `__cluster_metadata` ({@see \Protocol\Kafka\Protocol\Request\DescribeQuorumRequest}), so
 * this structure carries the index of that one partition and nothing else.
 *
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 and v1)"
 */
class DescribeQuorumRequestPartition implements BinarySchemaInterface
{
    /**
     * Index of the partition whose quorum is asked about
     */
    public int $partitionIndex;

    public function __construct(int $partitionIndex = 0)
    {
        $this->partitionIndex = $partitionIndex;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return ['partitionIndex' => BinarySchema::TYPE_INT32];
    }
}
