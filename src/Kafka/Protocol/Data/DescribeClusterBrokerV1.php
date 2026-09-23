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
 * One broker of a DescribeCluster answer of version 0 or 1: the four fields of KIP-700, without `is_fenced`
 *
 * Version 2 of the api (Kafka 4.0, KIP-1073) appended the `is_fenced` flag to every broker entry
 * ({@see DescribeClusterBroker}); this is the entry below it, which an answer of the version 0 or 1 carries. Its
 * {@see DescribeClusterBroker::$isFenced} stays false, because an answer of those versions never lists a fenced
 * broker.
 *
 * @see docs/protocol/4.3.md, section "The fenced brokers of KIP-1073 (v2)"
 */
final class DescribeClusterBrokerV1 extends DescribeClusterBroker
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
