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
 * @date   28.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * SyncGroup Request
 *
 * The sync group request is used by the group leader to assign state (e.g. partition assignments) to all members of
 * the current generation. All members send SyncGroup immediately after joining the group, but only the leader provides
 * the group's assignment.
 */
class SyncGroupRequest extends AbstractRequest
{
    /**
     * @param string $consumerGroup
     * @param int $generationId
     * @param string $memberId
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        private $consumerGroup,
        /**
         * The generation of the group.
         */
        private $generationId,
        /**
         * The member id assigned by the group coordinator.
         */
        private $memberId,
        /**
         * List of group member assignments
         */
        private readonly array $groupAssignments = [],
        $clientId = '',
        $correlationId = 0
    ) {
        parent::__construct(ApiKeys::SYNC_GROUP, $clientId, $correlationId);
    }

    /**
     * @inheritDoc
     *
     * SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
     *   GroupId => string
     *   GenerationId => int32
     *   MemberId => string
     *   GroupAssignment => [MemberId MemberAssignment]
     *     MemberId => string
     *     MemberAssignment => bytes

     */
    protected function packPayload(): string
    {
        $payload      = parent::packPayload();
        $groupLength  = strlen($this->consumerGroup);
        $memberLength = strlen($this->memberId);

        $payload .= pack(
            "na{$groupLength}Nna{$memberLength}N",
            $groupLength,
            $this->consumerGroup,
            $this->generationId,
            $memberLength,
            $this->memberId,
            count($this->groupAssignments)
        );

        foreach ($this->groupAssignments as $memberId => $memberAssignment) {
            $memberAssignment       = (string) $memberAssignment;
            $memberLength           = strlen($memberId);
            $memberAssignmentLength = strlen($memberAssignment);
            $payload .= pack(
                "na{$memberLength}N",
                $memberLength,
                $memberId,
                $memberAssignmentLength
            );
            $payload .= $memberAssignment;
        }

        return $payload;
    }
}
