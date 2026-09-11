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
 * OffsetFetch, version 0: the offsets are read from ZooKeeper, as they were in Kafka 0.8.1.
 *
 * The request is byte for byte the one of version 1 - only the version field of the header differs - so this class
 * only lowers the version constant of {@see OffsetFetchRequest}. Two consequences of reading from ZooKeeper are
 * visible to a client: any broker of the cluster answers the request, and a topic-partition without a committed
 * offset comes back with the error code 3 (UnknownTopicOrPartition) instead of a plain offset of -1.
 *
 * The nullable topic array of version 2 does not exist here either, see {@see OffsetFetchRequestV1}.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v5)"
 */
final class OffsetFetchRequestV0 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
