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

use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Heartbeat response, version 3.
 *
 * <pre>
 *   Heartbeat Response (Version: 1 to 3) => throttle_time_ms error_code
 *     throttle_time_ms => INT32     -- since version 1
 *     error_code       => INT16
 * </pre>
 *
 * The error code is the whole answer, and it is how the coordinator tells a member what to do next: 27
 * (RebalanceInProgress) means the group is rebalancing and the member has to send JoinGroup again, 25
 * (UnknownMemberId) that it was dropped out of the group, 22 (IllegalGeneration) that its generation is over.
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of it, as it did for every other api of the
 * group membership protocol; {@see HeartbeatResponseV0} is the six-byte answer without it, and
 * {@see HeartbeatResponseV1} the ten-byte answer of version 1, which version 2 (KIP-219, Kafka 2.0) and
 * version 3 (KIP-345, Kafka 2.3, whose `group_instance_id` is a field of the **request**) repeat byte for byte;
 * {@see HeartbeatResponseV2} is the same answer one api version lower. What version 3 did add is an error code
 * this answer can carry: **82** (`FencedInstanceId`), for a static member whose instance id another consumer has
 * taken over.
 *
 * @see docs/protocol/2.8.md, sections "Heartbeat API (key 12, v0 to v3)" and "Quotas and throttle time"
 */
class HeartbeatResponse extends AbstractResponse
{
    /**
     * Version of the Heartbeat API that this class decodes the answer of
     */
    public const int VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code.
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 1) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['errorCode'] = BinarySchema::TYPE_INT16;

        return $header + $body;
    }
}
