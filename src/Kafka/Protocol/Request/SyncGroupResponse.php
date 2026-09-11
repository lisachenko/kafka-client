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
 * SyncGroup response, version 2.
 *
 * <pre>
 *   SyncGroup Response (Version: 1 and 2) => throttle_time_ms error_code member_assignment
 *     throttle_time_ms  => INT32    -- since version 1
 *     error_code        => INT16
 *     member_assignment => BYTES
 * </pre>
 *
 * The assignment is the share of the member that sent the request, exactly the bytes the leader gave the
 * coordinator for it; a member the leader did not mention receives an empty byte array, and an answer with an error
 * code carries an empty one as well.
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the error code;
 * {@see SyncGroupResponseV0} is the answer without it and {@see SyncGroupResponseV1} the one of version 1, which
 * version 2 (KIP-219, Kafka 2.0) repeats byte for byte.
 *
 * @see docs/protocol/2.8.md, sections "SyncGroup API (key 14, v0 to v2)" and "Quotas and throttle time"
 */
class SyncGroupResponse extends AbstractResponse
{
    /**
     * Version of the SyncGroup API that this class decodes the answer of
     */
    public const int VERSION = 2;

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
     * Assignment of the member that sent the request, opaque to this api.
     */
    public string $memberAssignment;

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
        $body['errorCode']        = BinarySchema::TYPE_INT16;
        $body['memberAssignment'] = BinarySchema::TYPE_BYTEARRAY;

        return $header + $body;
    }
}
