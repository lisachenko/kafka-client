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
 * OffsetCommit response, version 6: the throttle time and the topics, the answer of every version from 3 on
 *
 * <pre>
 *   OffsetCommit Response (Version: 3 to 7) => throttle_time_ms [responses]
 * </pre>
 *
 * Version 7 (KIP-345, Kafka 2.3) added the `group_instance_id` to the **request** alone, so the five versions
 * decode the same bytes.
 *
 * @see docs/protocol/2.8.md, section "Static membership (KIP-345)"
 */
final class OffsetCommitResponseV6 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
