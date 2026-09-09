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
 * OffsetFetch response object, version 0: the answer of the offsets that live in ZooKeeper
 *
 * The frame is the one of version 1 ({@see OffsetFetchResponseV1}) - no group-level `error_code` - and only the
 * meaning of a partition without a committed offset differs: ZooKeeper has no node for it, so the broker reports
 * the offset -1 with the error code 3 (UnknownTopicOrPartition) where version 1 reports the offset -1 with the
 * error code 0.
 *
 * @see docs/protocol/0.11.0.md, section "OffsetFetch API (key 9, v0 to v3)"
 */
final class OffsetFetchResponseV0 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
