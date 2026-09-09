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
 * LeaveGroup, version 0: removes a member from its group without waiting for the session timeout.
 *
 * This is preferred over letting the session timeout expire, since it lets the group rebalance right away - for a
 * consumer that means that less time elapses before its partitions can be reassigned to an active member.
 *
 * <pre>
 *   LeaveGroup Request (Version: 0) => group_id member_id
 *     group_id  => STRING
 *     member_id => STRING
 * </pre>
 *
 * @see docs/protocol/0.10.2.md, section "LeaveGroup API (key 13, v0)"
 */
class LeaveGroupRequest extends AbstractRequest
{
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
        parent::__construct(ApiKeys::LEAVE_GROUP, $clientId, $correlationId);
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
