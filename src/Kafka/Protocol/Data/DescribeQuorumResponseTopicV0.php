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
 * One topic of a DescribeQuorum **v0** answer
 *
 * The entry itself is the one of {@see DescribeQuorumResponseTopic} - the name and its partitions - and what this
 * version selects is {@see DescribeQuorumResponsePartitionV0}, whose voters and observers have no timestamps.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
final class DescribeQuorumResponseTopicV0 extends DescribeQuorumResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
