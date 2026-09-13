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
 * SyncGroup response, version 5.
 *
 * <pre>
 *   SyncGroup Response (Version: 1 to 5) => throttle_time_ms error_code protocol_type protocol_name
 *                                             member_assignment
 *     throttle_time_ms  => INT32    -- since version 1
 *     error_code        => INT16
 *     protocol_type     => NULLABLE_STRING   -- since version 5
 *     protocol_name     => NULLABLE_STRING   -- since version 5
 *     member_assignment => BYTES
 * </pre>
 *
 * The assignment is the share of the member that sent the request, exactly the bytes the leader gave the
 * coordinator for it; a member the leader did not mention receives an empty byte array, and an answer with an error
 * code carries an empty one as well.
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the error code;
 * {@see SyncGroupResponseV0} is the answer without it and {@see SyncGroupResponseV1} the one of version 1, which
 * version 2 (KIP-219, Kafka 2.0) repeats byte for byte. Version 3 (KIP-345, Kafka 2.3) changed the **request**
 * alone - the `group_instance_id` of a static member - so {@see SyncGroupResponseV2} and {@see
 * SyncGroupResponseV4} decode the very same bytes as well. **Version 5 (KIP-559, Kafka 2.5) put the
 * `protocol_type` and the `protocol_name` of the generation between the error code and the assignment**, the
 * same pair the request of that version has to carry; both are null in an answer that reports an error, and both
 * name what the coordinator settled on in an answer that does not.
 *
 * @see docs/protocol/2.8.md, sections "SyncGroup API (key 14, v0 to v5)" and "Quotas and throttle time"
 */
class SyncGroupResponse extends AbstractResponse
{
    /**
     * Version of the SyncGroup API that this class decodes the answer of
     */
    public const int VERSION = 5;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 4;

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
     * The class of protocols of the group, the `protocol_type` of its members, or null when the group has none.
     *
     * @since Version 5 of protocol
     */
    public ?string $protocolType = null;

    /**
     * The protocol the coordinator selected for this generation, or null when the answer carries an error.
     *
     * @since Version 5 of protocol
     */
    public ?string $protocolName = null;

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
        $body['errorCode'] = BinarySchema::TYPE_INT16;
        if (static::VERSION >= 5) {
            $body['protocolType'] = BinarySchema::TYPE_NULLABLE_STRING;
            $body['protocolName'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $body['memberAssignment'] = BinarySchema::TYPE_BYTEARRAY;

        return $header + $body;
    }
}
