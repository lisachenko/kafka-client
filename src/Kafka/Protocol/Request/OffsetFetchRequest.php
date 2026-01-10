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

/**
 * This API describes the valid offset range available for a set of topic-partitions.
 *
 * As with the produce and fetch APIs requests must be directed to the broker that is currently the leader for the
 * partitions in question. This can be determined using the metadata API.
 *
 * The response contains the starting offset of each segment for the requested partition as well as the "log end
 * offset" i.e. the offset of the next message that would be appended to the given partition.
 */
class OffsetFetchRequest extends AbstractRequest
{
    /**
     * @param string $consumerGroup
     */
    public function __construct(/**
     * The consumer group id.
     */
        private $consumerGroup,
        private readonly array $topicPartitions,
        $clientId = '',
        $correlationId = 0
    ) {
        parent::__construct(ApiKeys::OFFSET_FETCH, $clientId, $correlationId, ApiKeys::VERSION_1);
    }

    /**
     * @inheritDoc
     */
    protected function packPayload(): string
    {
        $payload     = parent::packPayload();
        $groupLength = strlen($this->consumerGroup);
        $totalTopics = count($this->topicPartitions);

        $payload .= pack("na{$groupLength}N", $groupLength, $this->consumerGroup, $totalTopics);
        foreach ($this->topicPartitions as $topic => $partitions) {
            $topicLength = strlen($topic);
            $payload .= pack("na{$topicLength}N", $topicLength, $topic, count($partitions));
            $packArgs = $partitions;
            array_unshift($packArgs, 'N*');
            $payload .= call_user_func_array(pack(...), $packArgs);
        }

        return $payload;
    }
}
