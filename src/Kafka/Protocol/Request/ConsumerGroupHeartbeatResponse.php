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
use Protocol\Kafka\Protocol\Data\ConsumerGroupHeartbeatAssignment;
use Protocol\Kafka\Protocol\NullableStruct;

/**
 * ConsumerGroupHeartbeat response object, version 0 (key 68, Kafka 3.5, KIP-848)
 *
 * <pre>
 *   ConsumerGroupHeartbeat Response (Version: 0) => throttle_time_ms error_code error_message member_id
 *                                                   member_epoch heartbeat_interval_ms assignment
 *     throttle_time_ms      => INT32
 *     error_code            => INT16
 *     error_message         => COMPACT_NULLABLE_STRING
 *     member_id             => COMPACT_NULLABLE_STRING
 *     member_epoch          => INT32
 *     heartbeat_interval_ms => INT32
 *     assignment            => [topic_partitions]        -- NULLABLE struct
 * </pre>
 *
 * The answer carries three things a classic consumer had to piece together out of JoinGroup, SyncGroup and
 * Heartbeat:
 *
 * * the **member epoch**, which is the generation of this member and what an OffsetCommit v9 and an OffsetFetch
 *   v9 send in place of a generation id. A join (the request epoch 0) is answered with the epoch of the group,
 *   never with a 0 of its own;
 * * the **heartbeat interval** the coordinator dictates - `group.consumer.heartbeat.interval.ms` of the broker,
 *   not `heartbeat.interval.ms` of the client. A KIP-848 member has no say in how often it heartbeats, which is
 *   why {@see \Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator} reads it out of every answer;
 * * the **assignment**, a {@see NullableStruct} field: `null` means "nothing changed", an assignment with an
 *   empty array means "you own nothing", and neither is an error.
 *
 * `member_id` is only filled *"when the member joins with MemberEpoch == 0"* (`ConsumerGroupHeartbeatResponse.json`
 * @ 3.9.2), which is how a member that sent no id of its own learns the one the coordinator generated for it.
 *
 * The error codes of the api are the ten its specification lists, and four of them belong to KIP-848 alone: **110**
 * `FencedMemberEpoch` and **113** `StaleMemberEpoch` say "your epoch is not mine any more" - the member drops its
 * partitions and joins again with the epoch 0 - **112** `UnsupportedAssignor` refuses a `server_assignor` the group
 * does not have and **111** `UnreleasedInstanceId` a `group.instance.id` another member still holds; the last two
 * are configuration errors and no retry helps. **25** `UnknownMemberId` is the same rejoin as the 110, and the
 * three coordinator codes **14**, **15** and **16** are the retriable ones every group api has.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 */
class ConsumerGroupHeartbeatResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * The api is flexible from its first version: it was born after KIP-482 (Kafka 2.4)
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
     * Member id the coordinator generated, only filled when the member joined with the epoch 0
     */
    public ?string $memberId = null;

    /**
     * Epoch of the member, which is what OffsetCommit v9 and OffsetFetch v9 send in place of a generation
     */
    public int $memberEpoch = 0;

    /**
     * Interval in milliseconds at which the coordinator expects the next heartbeat of this member
     */
    public int $heartbeatIntervalMs = 0;

    /**
     * Partitions this member may own now, `null` when the assignment did not change since the last answer
     */
    public ?ConsumerGroupHeartbeatAssignment $assignment = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs'      => BinarySchema::TYPE_INT32,
            'errorCode'           => BinarySchema::TYPE_INT16,
            'errorMessage'        => BinarySchema::TYPE_NULLABLE_STRING,
            'memberId'            => BinarySchema::TYPE_NULLABLE_STRING,
            'memberEpoch'         => BinarySchema::TYPE_INT32,
            'heartbeatIntervalMs' => BinarySchema::TYPE_INT32,
            'assignment'          => new NullableStruct(ConsumerGroupHeartbeatAssignment::class),
        ];
    }
}
