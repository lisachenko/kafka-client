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
use Protocol\Kafka\Protocol\Data\LeaveGroupResponseMember;

/**
 * LeaveGroup response, version 3.
 *
 * <pre>
 *   LeaveGroup Response (Version: 1 and 2) => throttle_time_ms error_code
 *     throttle_time_ms => INT32     -- since version 1
 *     error_code       => INT16
 *
 *   LeaveGroup Response (Version: 3) => throttle_time_ms error_code [members]
 *     members => member_id group_instance_id error_code   -- since version 3
 *       member_id         => STRING
 *       group_instance_id => NULLABLE_STRING
 *       error_code        => INT16
 * </pre>
 *
 * A member id the coordinator does not know - because the member was already removed, or because the group does not
 * exist at all - is answered with the error code 25 (UnknownMemberId): in {@see self::$errorCode} below version 3,
 * and in the entry of that member from version 3 on.
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the error code;
 * {@see LeaveGroupResponseV0} is the answer without it, and {@see LeaveGroupResponseV1} the same ten bytes that
 * version 2 (KIP-219, Kafka 2.0) answers, which {@see LeaveGroupResponseV2} decodes.
 *
 * **Version 3 (Kafka 2.4, KIP-345) appended the member array** that belongs to the batch request of that version:
 * one entry per member of the request, in its order, with the error code of that member alone. What stays in
 * {@see self::$errorCode} is what applies to the **request** - 30 (`GroupAuthorizationFailed`), 15
 * (`GroupCoordinatorNotAvailable`), 16 (`NotCoordinatorForGroup`), 14 (`GroupLoadInProgress`) - and a request whose
 * members were all refused still carries **0** there, with the member array holding every reason.
 *
 * @see docs/protocol/2.8.md, sections "The batch leave of KIP-345 (v3)", "LeaveGroup API (key 13, v0 to v3)" and
 *      "Quotas and throttle time"
 */
class LeaveGroupResponse extends AbstractResponse
{
    /**
     * Version of the LeaveGroup API that this class decodes the answer of
     */
    public const int VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the request as a whole, 0 when only single members were refused.
     */
    public int $errorCode;

    /**
     * What became of every member of the batch, in the order of the request
     *
     * @var list<LeaveGroupResponseMember>
     *
     * @since Version 3 of protocol
     */
    public array $members = [];

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
        if (static::VERSION >= 3) {
            $body['members'] = [LeaveGroupResponseMember::class];
        }

        return $header + $body;
    }
}
