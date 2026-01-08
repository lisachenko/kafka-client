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
use Protocol\Kafka\Protocol\Data\OffsetCommitResponsePartition;

/**
 * OffsetCommit
 *
 * This api saves out the consumer's position in the stream for one or more partitions. In the scala API this happens
 * when the consumer calls commit() or in the background if "autocommit" is enabled. This is the position the consumer
 * will pick up from if it crashes before its next commit().
 */
class OffsetCommitRequest extends AbstractRequest
{
    /**
     * @param string $consumerGroup
     * @param int $generationId
     * @param string $memberName
     * @param int $retentionTime
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        private $consumerGroup,
        /**
         * The generation of the group.
         *
         * @since Version 1 of protocol
         */
        private $generationId,
        /**
         * The member id assigned by the group coordinator.
         *
         * @since Version 1 of protocol
         */
        private $memberName,
        /**
         * Time period in ms to retain the offset.
         *
         * @since Version 2 of protocol
         */
        private $retentionTime,
        private readonly array $topicPartitions,
        $clientId = '',
        $correlationId = 0
    ) {

        parent::__construct(ApiKeys::OFFSET_COMMIT, $clientId, $correlationId);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload      = parent::packPayload();
        $groupLength  = strlen($this->consumerGroup);
        $memberLength = strlen($this->memberName);
        $totalTopics  = count($this->topicPartitions);

        $payload .= pack(
            "na{$groupLength}Nna{$memberLength}JN",
            $groupLength,
            $this->consumerGroup,
            $this->generationId,
            $memberLength,
            $this->memberName,
            $this->retentionTime,
            $totalTopics
        );

        foreach ($this->topicPartitions as $topic => $partitions) {
            $topicLength = strlen($topic);
            $payload .= pack("na{$topicLength}N", $topicLength, $topic, count($partitions));
            /** @var OffsetCommitResponsePartition $partition */
            foreach ($partitions as $partition) {
                $payload .= (string) $partition;
            }
        }

        return $payload;
    }
}
