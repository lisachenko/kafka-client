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
use Protocol\Kafka\Protocol\Data\ShareGroupHeartbeatAssignment;
use Protocol\Kafka\Protocol\NullableStruct;

/**
 * ShareGroupHeartbeat response, version 1 (key 76, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ShareGroupHeartbeat Response (Version: 1) => throttle_time_ms error_code error_message member_id member_epoch
 *                                               heartbeat_interval_ms assignment
 *     assignment => [topic_partitions]     -- NULLABLE struct: ff "unchanged", 01 a structure follows
 * </pre>
 *
 * The answer of the consumer protocol, field for field ({@see ConsumerGroupHeartbeatResponse}). The interval is
 * `group.share.heartbeat.interval.ms` of the broker, and the assignment of a share group is computed by the `simple`
 * assignor of the coordinator, which hands one partition to several members at once. The errors are the top-level
 * code: the 42 of a frame that breaks a rule, the **25** of a member id the group does not hold, the 69 of a group
 * that is not a share group, the 30 of a principal that may not `READ` the group.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupHeartbeat API (key 76, v1)"
 */
class ShareGroupHeartbeatResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * The api is flexible from its first version
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Top-level error code, or 0 if there was no error
     */
    public int $errorCode = 0;

    /**
     * Top-level error message, or null if there was no error
     */
    public ?string $errorMessage = null;

    /**
     * Member id of the member, which the member generated and the coordinator echoes
     */
    public ?string $memberId = null;

    /**
     * Epoch of the member
     */
    public int $memberEpoch = 0;

    /**
     * Interval in milliseconds at which the coordinator expects the next heartbeat
     */
    public int $heartbeatIntervalMs = 0;

    /**
     * Partitions the member may fetch now, null when the assignment did not change
     */
    public ?ShareGroupHeartbeatAssignment $assignment = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return parent::getScheme() + [
            'throttleTimeMs'      => BinarySchema::TYPE_INT32,
            'errorCode'           => BinarySchema::TYPE_INT16,
            'errorMessage'        => BinarySchema::TYPE_NULLABLE_STRING,
            'memberId'            => BinarySchema::TYPE_NULLABLE_STRING,
            'memberEpoch'         => BinarySchema::TYPE_INT32,
            'heartbeatIntervalMs' => BinarySchema::TYPE_INT32,
            'assignment'          => new NullableStruct(ShareGroupHeartbeatAssignment::class),
        ];
    }
}
