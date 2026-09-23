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

/**
 * One topic of a DescribeQuorum **v1** answer
 *
 * The entry itself is the one of {@see DescribeQuorumResponseTopic} - the name and its partitions - and what this
 * version selects is {@see DescribeQuorumResponsePartitionV1}, the partition without the error message of KIP-853,
 * whose voters and observers carry the two timestamps of KIP-836 and no directory id.
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
final class DescribeQuorumResponseTopicV1 extends DescribeQuorumResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
