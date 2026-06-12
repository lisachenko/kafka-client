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
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Heartbeat Request
 *
 * Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group. If not
 * heartbeat has been received by the coordinator with the configured session timeout, the member will be kicked out of
 * the group.
 *
 * Heartbeat Request (Version: 0) => group_id generation_id member_id
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 */
class HeartbeatRequest extends AbstractRequest
{
    /**
     * @param string $consumerGroup
     * @param int $generationId
     * @param string $memberId
     */
    public function __construct(/**
     * The consumer group id.
     */
        private $consumerGroup, /**
     * The generation of the group.
     */
        private $generationId, /**
     * The member id assigned by the group coordinator.
     */
        private $memberId,
        $clientId = '',
        $correlationId = 0
    ) {
        parent::__construct(ApiKeys::HEARTBEAT, $clientId, $correlationId);
    }

    public static function getScheme(): array
    {
        $header = null;

        return $header + [
            'consumerGroup' => BinarySchema::TYPE_STRING,
            'generationId'  => BinarySchema::TYPE_INT32,
            'memberId'      => BinarySchema::TYPE_STRING,
        ];
    }
}
