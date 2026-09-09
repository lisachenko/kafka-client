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
 * JoinGroup, version 1: the request with which a client becomes a member of a group (Kafka 0.10.1)
 *
 * When new members join an existing group, all previous members are required to rejoin by sending a new join group
 * request. When a member first joins the group, the member id will be empty ({@see self::DEFAULT_MEMBER_ID}), and
 * the coordinator answers with the id it assigned to it; a rejoining member has to use the member id of the
 * previous generation, otherwise it is answered with the error code 25 (UnknownMemberId).
 *
 * <pre>
 *   JoinGroup Request (Version: 1) => group_id session_timeout rebalance_timeout member_id protocol_type
 *                                     [group_protocols]
 *     group_id          => STRING
 *     session_timeout   => INT32
 *     rebalance_timeout => INT32     -- since version 1
 *     member_id         => STRING
 *     protocol_type     => STRING
 *     group_protocols   => protocol_name protocol_metadata
 *       protocol_name     => STRING
 *       protocol_metadata => BYTES
 * </pre>
 *
 * Version 1 (KIP-62, Kafka 0.10.1) inserted `rebalance_timeout` **after** `session_timeout`, and that is its only
 * change; the response did not change at all. The two timeouts answer two different questions:
 *
 * * `session_timeout` is how long the coordinator keeps a member that does not send a heartbeat. It has to lie
 *   between `group.min.session.timeout.ms` and `group.max.session.timeout.ms` of the broker, otherwise the request
 *   is answered with the error code 26 (InvalidSessionTimeout).
 * * `rebalance_timeout` is how long the coordinator waits for **each** member to rejoin once a rebalance has
 *   started, i.e. for how long it holds every JoinGroup of that rebalance. `GroupMetadata.rebalanceTimeoutMs` @
 *   0.10.2.2 is the **largest** rebalance timeout of the members of the group, and `DelayedJoin` is scheduled with
 *   it. It is bounded by nothing: a value above `group.max.session.timeout.ms` is accepted, and so is 0.
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
 * @see docs/protocol/0.11.0.md, section "JoinGroup API (key 11, v0 and v1)"
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
    public const int VERSION = 1;

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
        int $correlationId = 0
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
        $body['memberId']       = BinarySchema::TYPE_STRING;
        $body['protocolType']   = BinarySchema::TYPE_STRING;
        $body['groupProtocols'] = ['name' => JoinGroupRequestProtocol::class];

        return $header + $body;
    }
}
