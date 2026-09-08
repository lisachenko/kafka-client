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
 * SyncGroup response, version 0.
 *
 * <pre>
 *   SyncGroup Response (Version: 0) => error_code member_assignment
 *     error_code        => INT16
 *     member_assignment => BYTES
 * </pre>
 *
 * The `throttle_time_ms` that opens this response on `main` is a field of version 1 (Kafka 0.10.1) and is therefore
 * absent here. The assignment is the share of the member that sent the request, exactly the bytes the leader gave
 * the coordinator for it; a member the leader did not mention receives an empty byte array, and an answer with an
 * error code carries an empty one as well.
 *
 * @see docs/protocol/0.9.0.md, section "SyncGroup API (key 14, v0)"
 */
class SyncGroupResponse extends AbstractResponse
{
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

        return $header + [
            'errorCode'        => BinarySchema::TYPE_INT16,
            'memberAssignment' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
