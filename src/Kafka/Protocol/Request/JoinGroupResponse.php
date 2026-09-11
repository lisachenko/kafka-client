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
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMemberV0;

/**
 * JoinGroup response, version 5.
 *
 * <pre>
 *   JoinGroup Response (Version: 2 to 5) => throttle_time_ms error_code generation_id group_protocol leader_id
 *                                      member_id [members]
 *     throttle_time_ms => INT32     -- since version 2
 *     error_code       => INT16
 *     generation_id    => INT32
 *     group_protocol   => STRING
 *     leader_id        => STRING
 *     member_id        => STRING
 *     members          => member_id group_instance_id member_metadata
 *       member_id         => STRING
 *       group_instance_id => NULLABLE_STRING   -- since version 5
 *       member_metadata   => BYTES
 * </pre>
 *
 * The member whose id equals {@see self::$leaderId} is the leader of this generation: it is the one that computes
 * the assignment out of {@see self::$members} and publishes it with a SyncGroup request. Every other member receives
 * an **empty** member array and only waits for its own assignment.
 *
 * The generation starts at 1 for the first generation of a group and is incremented on every rebalance. An answer
 * that carries an error code reports the generation **0** - not the `UNKNOWN_GENERATION_ID` of -1 that the Java
 * client uses, `GroupCoordinator.joinError` @ 0.11.0.3 builds it with `generationId = 0` - together with an empty
 * group protocol, an empty leader id, an empty member array and the member id that was sent.
 *
 * The order of {@see self::$members} is the order of the internal map of the coordinator and is **not** the order
 * in which the members joined; a leader that needs a stable order has to sort the array itself.
 *
 * The `rebalance_timeout` of the version 1 request did not change the answer at all: `JOIN_GROUP_RESPONSE_V1 =
 * JOIN_GROUP_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3, so {@see JoinGroupResponseV1} and
 * {@see JoinGroupResponseV0} decode the same bytes. Version 2 (KIP-124, Kafka 0.11) is the first one that changed
 * the answer, and only by the leading `throttle_time_ms`. Version 3 (KIP-219, Kafka 2.0) and version 4 (KIP-394,
 * Kafka 2.2) left it alone again, so {@see JoinGroupResponseV2}, {@see JoinGroupResponseV3} and
 * {@see JoinGroupResponseV4} decode the very same bytes. **Version 5 (KIP-345, Kafka 2.3) gave every entry of the
 * member array a nullable `group_instance_id`** behind its member id, so that the leader of a generation sees
 * which of its members are static ones - the `null` of a dynamic member is `ff ff` on the wire.
 *
 * **The 79 of KIP-394 is an ordinary error answer of this layout**: the generation -1, an empty group protocol, an
 * empty leader id and an empty member array - and {@see self::$memberId} holding the id the coordinator assigned
 * to the client, which is the whole point of it.
 *
 * @see docs/protocol/2.8.md, sections "JoinGroup API (key 11, v0 to v5)" and "Quotas and throttle time"
 */
class JoinGroupResponse extends AbstractResponse
{
    /**
     * Version of the JoinGroup API that this class decodes the answer of
     */
    public const int VERSION = 5;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 2 of protocol
     */
    public int $throttleTimeMs = 0;

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
        $body   = [];
        if (static::VERSION >= 2) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['errorCode']     = BinarySchema::TYPE_INT16;
        $body['generationId']  = BinarySchema::TYPE_INT32;
        $body['groupProtocol'] = BinarySchema::TYPE_STRING;
        $body['leaderId']      = BinarySchema::TYPE_STRING;
        $body['memberId']      = BinarySchema::TYPE_STRING;
        $body['members']       = ['memberId' => static::memberClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a member entry for the version of the API that this class decodes
     *
     * @return class-string<JoinGroupResponseMember>
     */
    protected static function memberClass(): string
    {
        return static::VERSION >= 5 ? JoinGroupResponseMember::class : JoinGroupResponseMemberV0::class;
    }
}
