<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * LeaveGroup Request
 *
 * To explicitly leave a group, the client can send a leave group request. This is preferred over letting the session
 * timeout expire since it allows the group to rebalance faster, which for the consumer means that less time will
 * elapse before partitions can be reassigned to an active member.
 *
 * LeaveGroup Request (Version: 0) => group_id member_id
 *   group_id => STRING
 *   member_id => STRING
 */
class LeaveGroupRequest extends AbstractRequest
{
    public function __construct(/**
     * The consumer group id.
     */
        private readonly string $consumerGroup, /**
     * The member id assigned by the group coordinator.
     */
        private readonly string $memberId,
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
        $header = null;

        return $header + [
            'consumerGroup' => BinarySchema::TYPE_STRING,
            'memberId'      => BinarySchema::TYPE_STRING,
        ];
    }
}
