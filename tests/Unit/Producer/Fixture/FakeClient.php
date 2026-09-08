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

namespace Protocol\Kafka\Tests\Unit\Producer\Fixture;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;

/**
 * A low-level client that answers produce requests from a script instead of from a broker.
 *
 * Every call to {@see FakeClient::produce()} is recorded, so that a test can assert how the producer batched the
 * records it was given, and is answered by the next entry of the list of behaviours the client was built with: a
 * closure that returns the result of the request or throws the error of it. Once the list runs out, every request is
 * acknowledged with growing offsets.
 */
final class FakeClient extends Client
{
    /**
     * Requests that this client received, in the order they were made
     *
     * @var list<array<string, array<int, list<Record>>>>
     */
    public array $produceCalls = [];

    /**
     * Behaviours of the next requests, each one a `fn(array $topicPartitionMessages): array`
     *
     * @var list<\Closure>
     */
    private array $behaviours;

    /**
     * Offset that the next acknowledged batch of a partition starts at
     *
     * @var array<string, int>
     */
    private array $nextOffsets = [];

    /**
     * @param array<string, mixed> $configuration Client configuration, as the producer would pass it
     * @param list<\Closure>       $behaviours    Answer of every request, in order
     */
    public function __construct(Cluster $cluster, array $configuration = [], array $behaviours = [])
    {
        $this->behaviours = $behaviours;

        parent::__construct($cluster, $configuration);
    }

    /**
     * @param array<string, array<int, list<Record>>> $topicPartitionMessages
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     */
    public function produce(array $topicPartitionMessages): array
    {
        $this->produceCalls[] = $topicPartitionMessages;

        $behaviour = array_shift($this->behaviours);
        if ($behaviour !== null) {
            return $behaviour($topicPartitionMessages);
        }

        return $this->acknowledge($topicPartitionMessages);
    }

    /**
     * Acknowledges every topic-partition of a request, with the offset the partition grew to
     *
     * @param array<string, array<int, list<Record>>> $topicPartitionMessages
     * @param int                                     $throttleTimeMs Delay the answer of a quota carries, as
     *        {@see Client::produce()} puts it on every partition of an answer
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     */
    public function acknowledge(array $topicPartitionMessages, int $throttleTimeMs = 0): array
    {
        $result = [];
        foreach ($topicPartitionMessages as $topic => $partitions) {
            foreach ($partitions as $partitionId => $records) {
                $baseOffset = $this->nextOffsets["{$topic}-{$partitionId}"] ?? 0;

                $this->nextOffsets["{$topic}-{$partitionId}"] = $baseOffset + count($records);

                $partitionResult                 = new ProduceResponsePartition();
                $partitionResult->partition      = $partitionId;
                $partitionResult->baseOffset     = $baseOffset;
                $partitionResult->throttleTimeMs = $throttleTimeMs;

                $result[$topic][$partitionId] = $partitionResult;
            }
        }

        return $result;
    }

    /**
     * Returns the records of every request this client received, flattened into a single list
     *
     * @return list<Record>
     */
    public function receivedRecords(): array
    {
        $records = [];
        foreach ($this->produceCalls as $topicPartitionMessages) {
            foreach ($topicPartitionMessages as $partitions) {
                foreach ($partitions as $partitionRecords) {
                    foreach ($partitionRecords as $record) {
                        $records[] = $record;
                    }
                }
            }
        }

        return $records;
    }
}
