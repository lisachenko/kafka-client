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
 * OffsetCommit response, version 3: the throttle time and the topics, the answer of version 4 as well
 *
 * <pre>
 *   OffsetCommit Response (Version: 3 and 4) => throttle_time_ms [responses]
 * </pre>
 *
 * Version 4 (KIP-219, Kafka 2.0) did not touch the answer either; only the meaning of a non-zero
 * `throttle_time_ms` changed, see {@see OffsetCommitRequestV3}.
 *
 * @see docs/protocol/2.8.md, sections "OffsetCommit API (key 8, v0 to v6)" and "Quotas and throttle time"
 */
final class OffsetCommitResponseV3 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
