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
 * JoinGroup, version 0: the request with which a client becomes a member of a group.
 *
 * When new members join an existing group, all previous members are required to rejoin by sending a new join group
 * request. When a member first joins the group, the member id will be empty ({@see self::DEFAULT_MEMBER_ID}), and
 * the coordinator answers with the id it assigned to it; a rejoining member has to use the member id of the
 * previous generation, otherwise it is answered with the error code 25 (UnknownMemberId).
 *
 * <pre>
 *   JoinGroup Request (Version: 0) => group_id session_timeout member_id protocol_type [group_protocols]
 *     group_id        => STRING
 *     session_timeout => INT32
 *     member_id       => STRING
 *     protocol_type   => STRING
 *     group_protocols => protocol_name protocol_metadata
 *       protocol_name     => STRING
 *       protocol_metadata => BYTES
 * </pre>
 *
 * Version 0 is the only version a Kafka 0.9.0.1 broker serves: the `rebalance_timeout` of version 1 arrived with
 * Kafka 0.10.1, and this broker answers a frame of that version with silence, see "An api the broker does not serve
 * is dropped, not refused" in the protocol document.
 *
 * **This request blocks.** The coordinator does not answer it until every known member of the group has rejoined,
 * i.e. until the rebalance is over or the session timeout of the members that did not show up has expired. The read
 * timeout of the connection - `request.timeout.ms` - therefore has to be larger than
 * {@see \Protocol\Kafka\Consumer\ConsumerConfig::SESSION_TIMEOUT_MS}, exactly as in the Java client.
 *
 * @see docs/protocol/0.9.0.md, section "JoinGroup API (key 11, v0)"
 */
class JoinGroupRequest extends AbstractRequest
{
    /**
     * Member id of a client that is not a member of the group yet, which asks the coordinator for one.
     *
     * `JoinGroupRequest.UNKNOWN_MEMBER_ID` @ 0.9.0.1.
     */
    public const string DEFAULT_MEMBER_ID = '';

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
     * @param string                                            $consumerGroup  The consumer group id
     * @param int                                               $sessionTimeout Session timeout in milliseconds
     * @param string                                            $memberId       Member id of the previous generation,
     *        {@see self::DEFAULT_MEMBER_ID} for a client that is joining for the first time
     * @param string                                            $protocolType   Class of protocols of the group
     * @param array<string, string|JoinGroupRequestProtocol>    $groupProtocols Metadata of each supported protocol
     * @param string                                            $clientId       Unique client identifier
     * @param int                                               $correlationId  Correlated request id
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

        parent::__construct(ApiKeys::JOIN_GROUP, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'consumerGroup'  => BinarySchema::TYPE_STRING,
            'sessionTimeout' => BinarySchema::TYPE_INT32,
            'memberId'       => BinarySchema::TYPE_STRING,
            'protocolType'   => BinarySchema::TYPE_STRING,
            'groupProtocols' => ['name' => JoinGroupRequestProtocol::class],
        ];
    }
}
