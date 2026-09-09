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
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMember;

/**
 * JoinGroup response, versions 0 and 1.
 *
 * <pre>
 *   JoinGroup Response (Version: 0 and 1) => error_code generation_id group_protocol leader_id member_id [members]
 *     error_code     => INT16
 *     generation_id  => INT32
 *     group_protocol => STRING
 *     leader_id      => STRING
 *     member_id      => STRING
 *     members        => member_id member_metadata
 *       member_id       => STRING
 *       member_metadata => BYTES
 * </pre>
 *
 * The member whose id equals {@see self::$leaderId} is the leader of this generation: it is the one that computes
 * the assignment out of {@see self::$members} and publishes it with a SyncGroup request. Every other member receives
 * an **empty** member array and only waits for its own assignment.
 *
 * The generation starts at 1 for the first generation of a group and is incremented on every rebalance. An answer
 * that carries an error code reports the generation **0** - not the `UNKNOWN_GENERATION_ID` of -1 that the Java
 * client uses, `GroupCoordinator.joinError` @ 0.10.2.2 builds it with `generationId = 0` - together with an empty
 * group protocol, an empty leader id, an empty member array and the member id that was sent.
 *
 * The order of {@see self::$members} is the order of the internal map of the coordinator and is **not** the order
 * in which the members joined; a leader that needs a stable order has to sort the array itself.
 *
 * The `rebalance_timeout` of the version 1 request did not change the answer at all: `JOIN_GROUP_RESPONSE_V1 =
 * JOIN_GROUP_RESPONSE_V0` in `Protocol.java` @ 0.10.2.2, so this one class decodes both versions.
 *
 * @see docs/protocol/0.11.0.md, section "JoinGroup API (key 11, v0 and v1)"
 */
class JoinGroupResponse extends AbstractResponse
{
    /**
     * Error code.
     */
    public int $errorCode;

    /**
     * The generation of the consumer group.
     */
    public int $generationId;

    /**
     * The group protocol selected by the coordinator out of the ones that every member supports.
     */
    public string $groupProtocol;

    /**
     * The member id of the leader of the group, which computes the assignment of this generation.
     */
    public string $leaderId;

    /**
     * The consumer id assigned by the group coordinator.
     */
    public string $memberId;

    /**
     * Members of the group with their metadata, filled for the leader only, indexed by the member id.
     *
     * @var array<string, JoinGroupResponseMember>
     */
    public array $members = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'     => BinarySchema::TYPE_INT16,
            'generationId'  => BinarySchema::TYPE_INT32,
            'groupProtocol' => BinarySchema::TYPE_STRING,
            'leaderId'      => BinarySchema::TYPE_STRING,
            'memberId'      => BinarySchema::TYPE_STRING,
            'members'       => ['memberId' => JoinGroupResponseMember::class],
        ];
    }
}
