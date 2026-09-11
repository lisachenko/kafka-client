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
 * Heartbeat, version 3: keeps a member of a group alive and tells it when a rebalance has started.
 *
 * Once a member has joined and synced it sends periodic heartbeats; if the coordinator receives none within the
 * `session_timeout` of the JoinGroup request, the member is removed from the group and the group rebalances. PHP has
 * no background thread, so this client sends the heartbeat from its poll loop, once
 * {@see \Protocol\Kafka\Consumer\ConsumerConfig::HEARTBEAT_INTERVAL_MS} has elapsed.
 *
 * <pre>
 *   Heartbeat Request (Version: 0 to 3) => group_id group_generation_id member_id group_instance_id
 *     group_id            => STRING
 *     group_generation_id => INT32
 *     member_id           => STRING
 *     group_instance_id   => NULLABLE_STRING   -- since version 3
 * </pre>
 *
 * `HEARTBEAT_REQUEST_V1 = HEARTBEAT_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124, Kafka 0.11)
 * changed the answer alone, which gained the leading `throttle_time_ms` ({@see HeartbeatResponse}), so a version 0
 * request ({@see HeartbeatRequestV0}) puts the same bytes on the wire and only reads its answer with
 * {@see HeartbeatResponseV0}. Version 2 (KIP-219, Kafka 2.0) repeated the exercise - the frame is untouched, only
 * the throttling contract of the answer changed - and {@see HeartbeatRequestV1} is that frame one api version
 * lower. **Version 3 (KIP-345, Kafka 2.3) is the first one that changed the frame**: it appended the nullable
 * `group_instance_id` of a static member, and it is the heartbeat that tells such a member that another instance
 * has taken its identity - the coordinator answers it 82 (`FencedInstanceId`), which is fatal for this
 * consumer. A dynamic member sends `null`, the frame {@see HeartbeatRequestV2} sends without the field.
 *
 * @see docs/protocol/2.8.md, section "Heartbeat API (key 12, v0 to v4)"
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
    public const int VERSION = 4;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 4;

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
        int $correlationId = 0,
        /**
         * Unique identifier of this consumer instance, `group.instance.id`, null for a dynamic member.
         *
         * @since Version 3 of protocol
         */
        protected readonly ?string $groupInstanceId = null
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        $body = [
            'consumerGroup' => BinarySchema::TYPE_STRING,
            'generationId'  => BinarySchema::TYPE_INT32,
            'memberId'      => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 3) {
            $body['groupInstanceId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $header + $body;
    }
}
