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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\JoinGroupRequestProtocol;

/**
 * JoinGroup, version 5: the request with which a client becomes a member of a group
 *
 * When new members join an existing group, all previous members are required to rejoin by sending a new join group
 * request. When a member first joins the group, the member id will be empty ({@see self::DEFAULT_MEMBER_ID}); a
 * rejoining member has to use the member id of the previous generation, otherwise it is answered with the error
 * code 25 (UnknownMemberId).
 *
 * **What an empty member id costs changed with version 4** (Kafka 2.2, KIP-394): the coordinator no longer adds a
 * member it cannot identify to a rebalance. It answers the join with the error code **79**
 * (`MemberIdRequired`) and the id it assigned, and the client has to send the very same request again with that
 * id - which is what {@see \Protocol\Kafka\Consumer\Internals\ConsumerCoordinator} does, immediately and
 * without a backoff, exactly as `AbstractCoordinator.handleJoinResponse` @ 2.8.2 does. A version 3 request with an
 * empty member id ({@see JoinGroupRequestV3}) is still added to the group at once.
 *
 * <pre>
 *   JoinGroup Request (Version: 1 to 5) => group_id session_timeout rebalance_timeout member_id
 *                                           group_instance_id protocol_type [group_protocols]
 *     group_id          => STRING
 *     session_timeout   => INT32
 *     rebalance_timeout => INT32     -- since version 1
 *     member_id         => STRING
 *     group_instance_id => NULLABLE_STRING  -- since version 5
 *     protocol_type     => STRING
 *     group_protocols   => protocol_name protocol_metadata
 *       protocol_name     => STRING
 *       protocol_metadata => BYTES
 * </pre>
 *
 * Version 1 (KIP-62, Kafka 0.10.1) inserted `rebalance_timeout` **after** `session_timeout`, and that is its only
 * change; the response did not change at all. Version 2 (KIP-124, Kafka 0.11) changed the request no further -
 * `JOIN_GROUP_REQUEST_V2 = JOIN_GROUP_REQUEST_V1` in `Protocol.java` @ 0.11.0.3 - and only added the leading
 * `throttle_time_ms` to the answer, which is why a version 1 request needs the answer class
 * {@see JoinGroupResponseV1} while this one is read with {@see JoinGroupResponse}. Version 3 (KIP-219, Kafka 2.0)
 * changed neither half once more - `JoinGroupRequest.json` @ 2.8.2 introduces nothing between the
 * `rebalance_timeout` of version 1 and the `group_instance_id` of version 5 - and only moved the api into the
 * throttling contract of KIP-219, where a throttled broker answers first and mutes the channel afterwards;
 * {@see JoinGroupRequestV2} is the same frame one api version lower.
 *
 * **Version 5 (KIP-345, Kafka 2.3) inserted the nullable `group_instance_id` behind the member id**: the
 * `group.instance.id` of a *static* member, with which a consumer keeps its identity - and therefore its
 * assignment - across a restart instead of being replaced by a new member id. A member that sends one and joins
 * an existing generation under an instance id that is already taken fences the older instance, which is answered
 * 82 (`FencedInstanceId`) from then on. A dynamic member sends `null` here, which is the frame of
 * {@see JoinGroupRequestV4} with one more field. The two timeouts answer two different questions:
 *
 * * `session_timeout` is how long the coordinator keeps a member that does not send a heartbeat. It has to lie
 *   between `group.min.session.timeout.ms` and `group.max.session.timeout.ms` of the broker, otherwise the request
 *   is answered with the error code 26 (InvalidSessionTimeout).
 * * `rebalance_timeout` is how long the coordinator waits for **each** member to rejoin once a rebalance has
 *   started, i.e. for how long it holds every JoinGroup of that rebalance. `GroupMetadata.rebalanceTimeoutMs` @
 *   0.10.2.2 is the **largest** rebalance timeout of the members of the group, and `DelayedJoin` is scheduled with
 *   it. It is bounded by nothing: a value above `group.max.session.timeout.ms` is accepted, and so is 0.
 *
 * **The metadata is opaque to the api but not to a 2.x coordinator.** From Kafka 2.3 on,
 * `GroupMetadata.computeSubscribedTopics()` parses the metadata of every member of a group whose `protocol_type`
 * is `consumer` as a `ConsumerProtocolSubscription`, and bytes it cannot parse wedge the group in
 * `PreparingRebalance` for good - see "A `consumer` group whose member metadata is not a Subscription never
 * rebalances" in the "Broker quirks and observations" section of the document. A member of a `consumer` group has
 * to send a real {@see \Protocol\Kafka\Consumer\Subscription} here; any other protocol type keeps the bytes
 * genuinely opaque.
 *
 * A version 0 request ({@see JoinGroupRequestV0}) has no such field, and the broker then uses the session timeout
 * as the rebalance timeout as well (`JoinGroupRequest.rebalanceTimeout` @ 0.10.2.2 falls back to
 * `sessionTimeout` for a struct without the field), which is exactly what a Kafka 0.9 coordinator did.
 *
 * On the client side this field is the wire half of `max.poll.interval.ms`
 * ({@see \Protocol\Kafka\Consumer\ConsumerConfig::MAX_POLL_INTERVAL_MS}).
 *
 * **This request blocks.** The coordinator does not answer it until every known member of the group has rejoined,
 * i.e. until the rebalance is over or the rebalance timeout of the members that did not show up has expired. The
 * read timeout of the connection - `request.timeout.ms` - therefore has to be larger than both the session timeout
 * and the rebalance timeout, exactly as in the Java client, whose `request.timeout.ms` defaults to 305000 against a
 * `max.poll.interval.ms` of 300000.
 *
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v6)"
 */
class JoinGroupRequest extends AbstractRequest
{
    /**
     * Member id of a client that is not a member of the group yet, which asks the coordinator for one.
     *
     * `JoinGroupRequest.UNKNOWN_MEMBER_ID` @ 0.10.2.2.
     */
    public const string DEFAULT_MEMBER_ID = '';

    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::JOIN_GROUP;

    /**
     * @inheritdoc
     */
    public const int VERSION = 6;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 6;

    /**
     * List of protocols that the member supports, indexed by the protocol name
     *
     * @var array<string, JoinGroupRequestProtocol>
     */
    protected readonly array $groupProtocols;

    /**
     * A value of the `$groupProtocols` map is either the raw metadata of that protocol or an already built
     * {@see JoinGroupRequestProtocol}; the metadata itself is opaque to this api.
     *
     * @param string                                            $consumerGroup    The consumer group id
     * @param int                                               $sessionTimeout   Session timeout in milliseconds
     * @param int                                               $rebalanceTimeout Rebalance timeout in milliseconds,
     *        ignored by the version 0 of the request
     * @param string                                            $memberId         Member id of the previous
     *        generation, {@see self::DEFAULT_MEMBER_ID} for a client that is joining for the first time
     * @param string                                            $protocolType     Class of protocols of the group
     * @param array<string, string|JoinGroupRequestProtocol>    $groupProtocols   Metadata of each supported protocol
     * @param string                                            $clientId         Unique client identifier
     * @param int                                               $correlationId    Correlated request id
     * @param string|null                                       $groupInstanceId  `group.instance.id` of a static
     *        member (KIP-345), null for a dynamic one
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
        /**
         * The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.
         *
         * The value has to lie between `group.min.session.timeout.ms` and `group.max.session.timeout.ms` of the
         * broker, otherwise the request is answered with the error code 26 (InvalidSessionTimeout).
         */
        protected readonly int $sessionTimeout,
        /**
         * The maximum time in ms that the coordinator waits for each member to rejoin when rebalancing the group.
         *
         * @since Version 1 of protocol
         */
        protected readonly int $rebalanceTimeout,
        /**
         * The member id assigned by the group coordinator.
         */
        protected readonly string $memberId,
        /**
         * Unique name for class of protocols implemented by group, e.g. `consumer`.
         */
        protected readonly string $protocolType,
        array $groupProtocols,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Unique identifier of this consumer instance, `group.instance.id`, null for a dynamic member.
         *
         * @since Version 5 of protocol
         */
        protected readonly ?string $groupInstanceId = null
    ) {
        $packedProtocols = [];
        foreach ($groupProtocols as $protocolName => $protocolMetadata) {
            $packedProtocols[$protocolName] = $protocolMetadata instanceof JoinGroupRequestProtocol
                ? $protocolMetadata
                : new JoinGroupRequestProtocol((string) $protocolName, $protocolMetadata);
        }
        $this->groupProtocols = $packedProtocols;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'consumerGroup'  => BinarySchema::TYPE_STRING,
            'sessionTimeout' => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 1) {
            $body['rebalanceTimeout'] = BinarySchema::TYPE_INT32;
        }
        $body['memberId'] = BinarySchema::TYPE_STRING;
        if (static::VERSION >= 5) {
            $body['groupInstanceId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $body['protocolType']   = BinarySchema::TYPE_STRING;
        $body['groupProtocols'] = ['name' => JoinGroupRequestProtocol::class];

        return $header + $body;
    }
}
