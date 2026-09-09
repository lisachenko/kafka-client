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

use Protocol\Kafka\Protocol\Data\JoinGroupRequestProtocol;

/**
 * JoinGroup, version 0: the request of Kafka 0.9, without the `rebalance_timeout` of version 1
 *
 * <pre>
 *   JoinGroup Request (Version: 0) => group_id session_timeout member_id protocol_type [group_protocols]
 * </pre>
 *
 * A 0.10.2.2 broker still serves this version, and it then treats the session timeout as the rebalance timeout of
 * the member as well - `JoinGroupRequest.rebalanceTimeout` @ 0.10.2.2 falls back to `sessionTimeout` when the
 * struct has no `rebalance_timeout` field. That is exactly how a Kafka 0.9 coordinator behaved, and it is why this
 * class passes the session timeout on as the rebalance timeout: the value is not written to the wire, but it
 * describes what the broker will do with the request.
 *
 * @see docs/protocol/0.11.0.md, section "JoinGroup API (key 11, v0, v1 and v2)"
 */
final class JoinGroupRequestV0 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param string                                         $consumerGroup  The consumer group id
     * @param int                                            $sessionTimeout Session timeout in milliseconds
     * @param string                                         $memberId       Member id of the previous generation,
     *        {@see self::DEFAULT_MEMBER_ID} for a client that is joining for the first time
     * @param string                                         $protocolType   Class of protocols of the group
     * @param array<string, string|JoinGroupRequestProtocol> $groupProtocols Metadata of each supported protocol
     * @param string                                         $clientId       Unique client identifier
     * @param int                                            $correlationId  Correlated request id
     */
    public function __construct(
        string $consumerGroup,
        int $sessionTimeout,
        string $memberId,
        string $protocolType,
        array $groupProtocols,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(
            $consumerGroup,
            $sessionTimeout,
            $sessionTimeout,
            $memberId,
            $protocolType,
            $groupProtocols,
            $clientId,
            $correlationId
        );
    }
}
