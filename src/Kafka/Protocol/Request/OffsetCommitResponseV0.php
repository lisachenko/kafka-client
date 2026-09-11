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
 * OffsetCommit response, version 0: the topic array alone, without the throttle time of version 3
 *
 * <pre>
 *   OffsetCommit Response (Version: 0, 1 and 2) => [responses]
 * </pre>
 *
 * This is the answer a broker gives to an offset commit that went to ZooKeeper (`offsets.storage = zookeeper`,
 * {@see OffsetCommitRequestV0}) as well; it is byte for byte the answer of {@see OffsetCommitResponseV1} and
 * {@see OffsetCommitResponseV2}.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v8)"
 */
final class OffsetCommitResponseV0 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
