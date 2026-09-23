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
 * DescribeCluster request of version 1 (Kafka 3.7, KIP-919): the endpoint type, without the fenced brokers
 *
 * The frame of {@see DescribeClusterRequest} **minus its last byte**: version 2 (Kafka 4.0, KIP-1073) appended
 * `include_fenced_brokers`, and this is the request below it. Its answer lists the brokers that are not fenced, and
 * the {@see DescribeClusterRequest::$includeFencedBrokers} of an instance of this class never reaches the wire.
 *
 * @see docs/protocol/4.3.md, sections "The endpoint type of KIP-919 (v1)" and "The fenced brokers of KIP-1073 (v2)"
 */
final class DescribeClusterRequestV1 extends DescribeClusterRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
