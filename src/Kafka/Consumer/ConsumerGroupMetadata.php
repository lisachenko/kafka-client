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

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;

/**
 * Who a consumer is inside its group, as a transactional producer has to tell the coordinator (KIP-447, Kafka 2.5)
 *
 * `org.apache.kafka.clients.consumer.ConsumerGroupMetadata` @ 2.5.1, and the second argument of the Java
 * `Producer.sendOffsetsToTransaction(Map, ConsumerGroupMetadata)`. Until Kafka 2.5 a transactional producer told
 * the group coordinator the **group id** alone, and the coordinator had no way of telling a producer of the current
 * generation from one of a generation that is over: a consumer that had already been rebalanced away could still
 * commit offsets into a transaction. The version 3 of TxnOffsetCommit carries the generation, the member id and the
 * group instance id as well, and `GroupCoordinator.handleTxnCommitOffsets` @ 2.8.2 refuses a stale one with 22
 * (IllegalGeneration), 25 (UnknownMemberId) or 82 (FencedInstanceId).
 *
 * {@see self::forGroup()} builds the "I am not in a group" form that the versions below 3 carry implicitly: the
 * generation -1 and the empty member id, which the coordinator accepts exactly as it accepted a v2 commit.
 *
 * @see docs/protocol/2.8.md, section "The consumer group metadata of a transactional commit (KIP-447)"
 */
final class ConsumerGroupMetadata implements \Stringable
{
    /**
     * @param string      $groupId         Id of the consumer group whose offsets are committed
     * @param int         $generationId    Generation the consumer belongs to, or {@see OffsetCommitRequest::DEFAULT_GENERATION_ID}
     * @param string      $memberId        Member id the coordinator assigned, the empty string when there is none
     * @param string|null $groupInstanceId `group.instance.id` of a static member (KIP-345), `null` for a dynamic one
     */
    public function __construct(
        public readonly string $groupId,
        public readonly int $generationId = OffsetCommitRequest::DEFAULT_GENERATION_ID,
        public readonly string $memberId = '',
        public readonly ?string $groupInstanceId = null
    ) {}

    /**
     * The metadata of a producer that commits for a group without being a member of it
     *
     * This is what a TxnOffsetCommit below the version 3 always meant, and what the coordinator still accepts: the
     * generation -1 with an empty member id skips the membership check of `handleTxnCommitOffsets` @ 2.8.2.
     */
    public static function forGroup(string $groupId): self
    {
        return new self($groupId);
    }

    public function __toString(): string
    {
        return 'ConsumerGroupMetadata{groupId=' . $this->groupId
            . ', generationId=' . $this->generationId
            . ", memberId='" . $this->memberId . "'"
            . ($this->groupInstanceId === null ? '' : ", groupInstanceId='" . $this->groupInstanceId . "'")
            . '}';
    }
}
