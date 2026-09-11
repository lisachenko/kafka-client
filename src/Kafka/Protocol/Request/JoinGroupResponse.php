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
 * JoinGroup response, version 7.
 *
 * <pre>
 *   JoinGroup Response (Version: 2 to 7) => throttle_time_ms error_code generation_id protocol_type
 *                                      group_protocol leader_id member_id [members]
 *     throttle_time_ms => INT32     -- since version 2
 *     error_code       => INT16
 *     generation_id    => INT32
 *     protocol_type    => NULLABLE_STRING   -- since version 7
 *     group_protocol   => STRING, NULLABLE_STRING since version 7
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
 * **Version 7 (KIP-559, Kafka 2.5) tells the client what the group runs on**: the answer opens a nullable
 * `protocol_type` in front of the protocol name - the very `protocol_type` the members sent, `consumer` for a
 * consumer group - and the `protocol_name` itself became nullable with it. The request did not change at all
 * ("Version 7 is the same as version 6"), so {@see JoinGroupRequestV6} and {@see JoinGroupRequest} put the same
 * bytes on the wire and only {@see JoinGroupResponseV6} reads one field less. What the client gains is a check
 * it could not make before: an answer whose `protocol_type` is not the one it asked for belongs to a group of
 * another kind. `KafkaApis.handleJoinGroupRequest` @ 2.8.2 is where the two fields part company from the
 * versions below: `joinResult.protocolName.orNull` for a version 7 and `.getOrElse(NoProtocol)` - the **empty
 * string** - for everything below it, so an error answer of this version carries `null` twice where a version 6
 * one carries `""` once.
 *
 * **The 79 of KIP-394 is an ordinary error answer of this layout**: the generation -1, an empty group protocol, an
 * empty leader id and an empty member array - and {@see self::$memberId} holding the id the coordinator assigned
 * to the client, which is the whole point of it.
 *
 * @see docs/protocol/2.8.md, sections "JoinGroup API (key 11, v0 to v7)" and "Quotas and throttle time"
 */
class JoinGroupResponse extends AbstractResponse
{
    /**
     * Version of the JoinGroup API that this class decodes the answer of
     */
    public const int VERSION = 7;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 6;

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
     * The class of protocols of the group, the `protocol_type` of the join, or null when the answer has none.
     *
     * The field is the half of KIP-559 that the broker sends back: an answer of a group that has settled on a
     * protocol carries the very `protocol_type` its members sent, and an **error** answer - or an answer of a
     * group that has none yet - carries `null`.
     *
     * @since Version 7 of protocol
     */
    public ?string $protocolType = null;

    /**
     * The group protocol selected by the coordinator out of the ones that every member supports.
     *
     * **The field is nullable from version 7 on** (KIP-559): up to version 6 an error answer carries the empty
     * string here, the `GroupCoordinator.NoProtocol` of `KafkaApis.handleJoinGroupRequest` @ 2.8.2, and from
     * version 7 on the very same answer carries `null` instead.
     */
    public ?string $groupProtocol = null;

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
        $body['errorCode']    = BinarySchema::TYPE_INT16;
        $body['generationId'] = BinarySchema::TYPE_INT32;
        if (static::VERSION >= 7) {
            $body['protocolType'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $body['groupProtocol'] = static::VERSION >= 7
            ? BinarySchema::TYPE_NULLABLE_STRING
            : BinarySchema::TYPE_STRING;
        $body['leaderId'] = BinarySchema::TYPE_STRING;
        $body['memberId'] = BinarySchema::TYPE_STRING;
        $body['members']  = ['memberId' => static::memberClass()];

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
