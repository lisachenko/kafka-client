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
 * DescribeCluster answer of version 0 (Kafka 2.8, KIP-700): the cluster without the endpoint type of KIP-919
 *
 * The frame of {@see DescribeClusterResponse} **minus one byte**: version 1 (Kafka 3.7) inserted the
 * `endpoint_type` between the error message and the cluster id, and this is the answer below it, which always
 * describes the brokers because {@see DescribeClusterRequestV0} can ask for nothing else.
 *
 * @see docs/protocol/3.9.md, sections "DescribeCluster API (key 60, v0 and v1)" and "The endpoint type of KIP-919
 *      (v1)"
 */
final class DescribeClusterResponseV0 extends DescribeClusterResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
