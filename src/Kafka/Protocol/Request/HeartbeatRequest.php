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

/**
 * Heartbeat, version 1: keeps a member of a group alive and tells it when a rebalance has started.
 *
 * Once a member has joined and synced it sends periodic heartbeats; if the coordinator receives none within the
 * `session_timeout` of the JoinGroup request, the member is removed from the group and the group rebalances. PHP has
 * no background thread, so this client sends the heartbeat from its poll loop, once
 * {@see \Protocol\Kafka\Consumer\ConsumerConfig::HEARTBEAT_INTERVAL_MS} has elapsed.
 *
 * <pre>
 *   Heartbeat Request (Version: 0 and 1) => group_id group_generation_id member_id
 *     group_id            => STRING
 *     group_generation_id => INT32
 *     member_id           => STRING
 * </pre>
 *
 * `HEARTBEAT_REQUEST_V1 = HEARTBEAT_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124, Kafka 0.11)
 * changed the answer alone, which gained the leading `throttle_time_ms` ({@see HeartbeatResponse}), so a version 0
 * request ({@see HeartbeatRequestV0}) puts the same bytes on the wire and only reads its answer with
 * {@see HeartbeatResponseV0}.
 *
 * @see docs/protocol/0.11.0.md, section "Heartbeat API (key 12, v0 and v1)"
 */
class HeartbeatRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::HEARTBEAT;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
        /**
         * The generation of the group.
         */
        protected readonly int $generationId,
        /**
         * The member id assigned by the group coordinator.
         */
        protected readonly string $memberId,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'consumerGroup' => BinarySchema::TYPE_STRING,
            'generationId'  => BinarySchema::TYPE_INT32,
            'memberId'      => BinarySchema::TYPE_STRING,
        ];
    }
}
