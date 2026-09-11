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
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerGroupMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Producer\Internals\ProducerIdAndEpoch;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\InitProducerIdRequest;

/**
 * A low-level client that answers produce requests from a script instead of from a broker.
 *
 * Every produce request is recorded, so that a test can assert how the producer batched the records it was given,
 * and is answered by the next entry of the list of behaviours the client was built with: a closure that returns the
 * result of the request or throws the error of it. Once the list runs out, every request is acknowledged with
 * growing offsets.
 *
 * The double sits **below** the producer state of KIP-98: it replaces {@see Client::produceRecords()}, the method
 * that turns records into a request, so the whole bookkeeping of {@see Client::produce()} - asking for a producer
 * id, numbering the batches, moving the sequences on and reacting to the error codes 45, 46 and 47 - is the real
 * one even in a unit test. Only the `InitProducerId` request is scripted as well, by
 * {@see FakeClient::$producerIds}.
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
     * Producer state of every produce request, in the order they were made
     *
     * @var list<array{producerId: int, producerEpoch: int, baseSequences: array<string, array<int, int>>,
     *          transactionalId: string|null}>
     */
    public array $producerStates = [];

    /**
     * Arguments of every `InitProducerId` request, in the order they were made
     *
     * @var list<array{transactionalId: string|null, transactionTimeoutMs: int}>
     */
    public array $initProducerIdCalls = [];

    /**
     * Producer ids that the scripted `InitProducerId` hands out, one after the other
     *
     * @var list<ProducerIdAndEpoch>
     */
    public array $producerIds = [];

    /**
     * Errors the scripted `InitProducerId` throws before it hands out an id, one after the other
     *
     * @var list<\Throwable>
     */
    public array $initProducerIdErrors = [];

    /**
     * Every call of one of the four transaction apis, in the order they were made
     *
     * @var list<array<int, mixed>>
     */
    public array $transactionCalls = [];

    /**
     * Errors the transaction apis throw, per api name and in order
     *
     * @var array<string, list<\Throwable>>
     */
    public array $transactionErrors = [
        'addPartitionsToTxn' => [],
        'addOffsetsToTxn'    => [],
        'endTxn'             => [],
        'txnOffsetCommit'    => [],
    ];

    /**
     * Every coordinator lookup, as `[kind, key]` pairs
     *
     * @var list<array{string, string}>
     */
    public array $coordinatorLookups = [];

    /**
     * The node that every coordinator lookup of this client answers with
     */
    public Node $coordinator;

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
        $this->behaviours  = $behaviours;
        $this->coordinator = new Node();

        parent::__construct($cluster, $configuration);
    }

    /**
     * @param array<string, array<int, iterable<Record|string|\Stringable>>> $topicPartitionMessages
     * @param array<string, array<int, int>>                                 $baseSequences
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     */
    protected function produceRecords(
        array $topicPartitionMessages,
        int $producerId = RecordBatch::NO_PRODUCER_ID,
        int $producerEpoch = RecordBatch::NO_PRODUCER_EPOCH,
        array $baseSequences = [],
        ?string $transactionalId = null
    ): array {
        $records = [];
        foreach ($topicPartitionMessages as $topic => $partitionMessages) {
            foreach ($partitionMessages as $partition => $messages) {
                foreach ($messages as $message) {
                    $records[$topic][$partition][] = $message instanceof Record
                        ? $message
                        : new Record((string) $message);
                }
            }
        }

        $this->produceCalls[]   = $records;
        $this->producerStates[] = [
            'producerId'      => $producerId,
            'producerEpoch'   => $producerEpoch,
            'baseSequences'   => $baseSequences,
            'transactionalId' => $transactionalId,
        ];

        $behaviour = array_shift($this->behaviours);
        if ($behaviour !== null) {
            return $behaviour($records);
        }

        return $this->acknowledge($records);
    }

    /**
     * Hands out the next scripted producer id instead of asking a broker for one
     */
    public function initProducerId(
        ?string $transactionalId = null,
        int $transactionTimeoutMs = InitProducerIdRequest::DEFAULT_TRANSACTION_TIMEOUT_MS,
        int $producerId = InitProducerIdRequest::NO_PRODUCER_ID,
        int $producerEpoch = InitProducerIdRequest::NO_PRODUCER_EPOCH
    ): ProducerIdAndEpoch {
        $this->initProducerIdCalls[] = [
            'transactionalId'      => $transactionalId,
            'transactionTimeoutMs' => $transactionTimeoutMs,
            // The pair of KIP-360: -1/-1 asks for a new id, anything else asks for an epoch bump
            'producerId'           => $producerId,
            'producerEpoch'        => $producerEpoch,
        ];

        $error = array_shift($this->initProducerIdErrors);
        if ($error !== null) {
            throw $error;
        }

        return array_shift($this->producerIds) ?? new ProducerIdAndEpoch(1000, 0);
    }

    /**
     * Answers the coordinator lookup of a transactional id without asking a broker
     */
    public function getTransactionCoordinator(string $transactionalId): Node
    {
        $this->coordinatorLookups[] = ['transaction', $transactionalId];

        return $this->coordinator;
    }

    /**
     * Answers the coordinator lookup of a consumer group without asking a broker
     */
    public function getGroupCoordinator(string $groupId): Node
    {
        $this->coordinatorLookups[] = ['group', $groupId];

        return $this->coordinator;
    }

    /**
     * Records an `AddPartitionsToTxn` and throws the next scripted error of that api
     */
    public function addPartitionsToTxn(
        Node $coordinatorNode,
        string $transactionalId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        array $topicPartitions
    ): void {
        $this->transactionCalls[] = ['addPartitionsToTxn', $transactionalId, $topicPartitions];

        $this->maybeThrow('addPartitionsToTxn');
    }

    /**
     * Records an `AddOffsetsToTxn` and throws the next scripted error of that api
     */
    public function addOffsetsToTxn(
        Node $coordinatorNode,
        string $transactionalId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        string $groupId
    ): void {
        $this->transactionCalls[] = ['addOffsetsToTxn', $transactionalId, $groupId];

        $this->maybeThrow('addOffsetsToTxn');
    }

    /**
     * Records an `EndTxn` and throws the next scripted error of that api
     */
    public function endTxn(
        Node $coordinatorNode,
        string $transactionalId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        bool $transactionResult
    ): void {
        $this->transactionCalls[] = ['endTxn', $transactionalId, $transactionResult];

        $this->maybeThrow('endTxn');
    }

    /**
     * Records a `TxnOffsetCommit` and throws the next scripted error of that api
     */
    public function txnOffsetCommit(
        Node $coordinatorNode,
        string $transactionalId,
        string $groupId,
        ProducerIdAndEpoch $producerIdAndEpoch,
        array $topicPartitionOffsets,
        ?ConsumerGroupMetadata $groupMetadata = null
    ): void {
        // The membership of KIP-447 is recorded as well, so that a test can see what the producer told the group
        // coordinator about its consumer
        $this->transactionCalls[] = [
            'txnOffsetCommit',
            $transactionalId,
            $groupId,
            $topicPartitionOffsets,
            $groupMetadata,
        ];

        $this->maybeThrow('txnOffsetCommit');
    }

    /**
     * Throws the next scripted error of one of the transaction apis, if there is one left
     */
    private function maybeThrow(string $api): void
    {
        $error = array_shift($this->transactionErrors[$api]);
        if ($error !== null) {
            throw $error;
        }
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
