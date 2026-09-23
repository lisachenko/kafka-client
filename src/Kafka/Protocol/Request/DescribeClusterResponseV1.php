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

namespace Protocol\Kafka\Protocol\Request;

/**
 * DescribeCluster answer of version 1 (Kafka 3.7, KIP-919): the endpoint type, and brokers without `is_fenced`
 *
 * The frame of {@see DescribeClusterResponse} with one byte less in every broker entry: version 2 (Kafka 4.0,
 * KIP-1073) appended the `is_fenced` flag to a broker, and this is the answer below it, whose brokers are
 * {@see \Protocol\Kafka\Protocol\Data\DescribeClusterBrokerV1} entries and whose list never names a fenced one.
 *
 * @see docs/protocol/4.3.md, sections "The endpoint type of KIP-919 (v1)" and "The fenced brokers of KIP-1073 (v2)"
 */
final class DescribeClusterResponseV1 extends DescribeClusterResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
