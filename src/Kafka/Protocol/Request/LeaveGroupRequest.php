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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * LeaveGroup Request
 *
 * To explicitly leave a group, the client can send a leave group request. This is preferred over letting the session
 * timeout expire since it allows the group to rebalance faster, which for the consumer means that less time will
 * elapse before partitions can be reassigned to an active member.
 */
class LeaveGroupRequest extends AbstractRequest
{
    /**
     * @param string $consumerGroup
     * @param string $memberId
     */
    public function __construct(/**
     * The consumer group id.
     */
        private $consumerGroup, /**
     * The member id assigned by the group coordinator.
     */
        private $memberId,
        $correlationId = 0,
        $clientId = ''
    ) {
        parent::__construct(ApiKeys::LEAVE_GROUP, $correlationId, $clientId);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload      = parent::packPayload();
        $groupLength  = strlen($this->consumerGroup);
        $memberLength = strlen($this->memberId);

        $payload .= pack(
            "na{$groupLength}na{$memberLength}",
            $groupLength,
            $this->consumerGroup,
            $memberLength,
            $this->memberId
        );

        return $payload;
    }
}
