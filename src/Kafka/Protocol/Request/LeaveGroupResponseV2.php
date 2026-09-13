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
 * LeaveGroup response, version 2: the throttle time and the error code of the one member that left
 *
 * <pre>
 *   LeaveGroup Response (Version: 1 and 2) => throttle_time_ms error_code
 * </pre>
 *
 * Version 3 (Kafka 2.4, KIP-345) appended the member array of the batch it answers, see
 * {@see LeaveGroupResponse}; below it the error code of the single member *is* the error code of the answer.
 *
 * @see docs/protocol/2.8.md, section "The batch leave of KIP-345 (v3)"
 */
final class LeaveGroupResponseV2 extends LeaveGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
