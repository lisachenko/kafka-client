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
use Protocol\Kafka\Protocol\Data\DescribeProducersRequestTopic;

/**
 * DescribeProducers, version 0: the producer state of a partition (ApiKey 61, Kafka 2.8, KIP-664)
 *
 * <pre>
 *   DescribeProducers Request (Version: 0) => [topics]
 *     topics => name [partition_indexes]
 *       name             => COMPACT_STRING
 *       partition_indexes => COMPACT_ARRAY of INT32
 * </pre>
 *
 * KIP-664 - *"Provide tooling to detect and abort hanging transactions"* - made the producer state that every log
 * keeps **visible**: which producer ids wrote to a partition, with which epoch and sequence number, and whether
 * one of them has a transaction still open in it. Before Kafka 2.8 that state could only be read by dumping the
 * log segments with `kafka-run-class.sh kafka.tools.DumpLogSegments`.
 *
 * **The request goes to the leader of each partition**: the state lives in the log, so a broker that does not
 * lead the partition answers it with 3 (`UnknownTopicOrPartition`) and not with 6.
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
class DescribeProducersRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_PRODUCERS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics of this request, indexed by their name
     *
     * @var array<string, DescribeProducersRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param array<string, list<int>> $topicPartitions Partitions to describe, per topic
     * @param string                   $clientId        A user specified identifier for the client
     * @param int                      $correlationId   A value the broker passes back unmodified
     */
    public function __construct(array $topicPartitions, string $clientId = '', int $correlationId = 0)
    {
        $topics = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $topics[$topic] = new DescribeProducersRequestTopic((string) $topic, array_values($partitions));
        }
        $this->topics = $topics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['name' => DescribeProducersRequestTopic::class],
        ];
    }

    /**
     * Returns the topics of this request, indexed by their name
     *
     * @return array<string, DescribeProducersRequestTopic>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
