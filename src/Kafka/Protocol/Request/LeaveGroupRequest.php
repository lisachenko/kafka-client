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
 * LeaveGroup, version 2: removes a member from its group without waiting for the session timeout.
 *
 * This is preferred over letting the session timeout expire, since it lets the group rebalance right away - for a
 * consumer that means that less time elapses before its partitions can be reassigned to an active member.
 *
 * <pre>
 *   LeaveGroup Request (Version: 0, 1 and 2) => group_id member_id
 *     group_id  => STRING
 *     member_id => STRING
 * </pre>
 *
 * `LEAVE_GROUP_REQUEST_V1 = LEAVE_GROUP_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124, Kafka 0.11)
 * changed the answer alone ({@see LeaveGroupResponse}), so {@see LeaveGroupRequestV0} sends the same bytes, and
 * version 2 (KIP-219, Kafka 2.0) changed nothing at all but the throttling contract, so {@see LeaveGroupRequestV1}
 * sends them too. The api only changes again at version 3 (KIP-345, Kafka 2.4), where the single `member_id`
 * becomes a batch of member identities - a version of a later release of this major.
 *
 * @see docs/protocol/2.8.md, section "LeaveGroup API (key 13, v0 to v2)"
 */
class LeaveGroupRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::LEAVE_GROUP;

    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
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
            'memberId'      => BinarySchema::TYPE_STRING,
        ];
    }
}
