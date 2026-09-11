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
 * OffsetCommit response, version 4: the throttle time and the topics, the answer of every version from 3 on
 *
 * <pre>
 *   OffsetCommit Response (Version: 3 to 6) => throttle_time_ms [responses]
 * </pre>
 *
 * Neither KIP-211 (version 5, which removes a field from the REQUEST) nor KIP-320 (version 6, which adds one to
 * the request's partitions) touched the answer, so the four versions decode the same bytes.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v8)"
 */
final class OffsetCommitResponseV4 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
