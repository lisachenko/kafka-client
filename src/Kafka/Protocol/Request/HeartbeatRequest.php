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
 * Heartbeat Request
 *
 * Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group. If not
 * heartbeat has been received by the coordinator with the configured session timeout, the member will be kicked out of
 * the group.
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

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload      = parent::packPayload();
        $groupLength  = strlen($this->consumerGroup);
        $memberLength = strlen($this->memberId);

        $payload .= pack(
            "na{$groupLength}Nna{$memberLength}",
            $groupLength,
            $this->consumerGroup,
            $this->generationId,
            $memberLength,
            $this->memberId
        );

        return $payload;
    }
}
