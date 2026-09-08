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

namespace Protocol\Kafka\Producer\Internals;

use Protocol\Kafka\Client;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Network\ResponseValidator;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

/**
 * The produce path of a {@see KafkaProducer} that was configured with a `compression.type` other than `none`.
 *
 * A compressed batch is a message set of exactly one message, whose value is the whole batch compressed with the
 * configured codec and whose attributes announce that codec; {@see MessageSet::fromRecords()} builds it, the broker
 * stores it as it is, and a consumer gets the records it holds back out of it.
 *
 * This class only exists because {@see Client::produce()} always builds *uncompressed* message sets out of the
 * records it is given and can not be told which codec to use. Teaching the client that codec is a one-line change
 * of its `produce()`:
 *
 * <code>
 *   MessageSet::fromRecords(
 *       self::toRecords($messages),
 *       ProducerConfig::compressionCodec($this->configuration[ProducerConfig::COMPRESSION_TYPE] ?? 'none')
 *   );
 * </code>
 *
 * Once that is in place this class has no reason to exist any more and {@see KafkaProducer::createClient()} can hand
 * out a plain {@see Client} for every codec.
 *
 * Everything else follows the client: the request carries a correlation id that the answer is checked against, and a
 * batch whose partitions failed with a retriable error is sent again - only for those partitions - after a refresh
 * of the cluster metadata, `retries` times with `retry.backoff.ms` in between.
 *
 * @see docs/protocol/0.8.2.md, section "Produce API (key 0, v0)"
 */
final class CompressingClient extends Client
{
    /**
     * @param Cluster              $cluster          Cluster the records are produced to
     * @param array<string, mixed> $configuration    Producer configuration, {@see ProducerConfig}
     * @param int                  $compressionCodec Codec of every batch, one of the {@see CompressionCodec} values
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly array $configuration,
        private readonly int $compressionCodec
    ) {
        parent::__construct($cluster, $configuration);
    }

    /**
     * Compresses the records of every topic-partition into a message set and appends them to their partition
     *
     * @param array<string, array<int, iterable<Record>>> $topicPartitionMessages Records per topic and partition
     *
     * @return array<string, array<int, ProduceResponsePartition>> Accepted partitions in the form
     *         [topic => [partition => ProduceResponsePartition]], empty for a fire-and-forget request (acks = 0),
     *         which the broker never answers
     *
     * @throws TopicPartitionRequestException If the request only succeeded on some of the topic-partitions
     */
    public function produce(array $topicPartitionMessages): array
    {
        $pendingMessageSets = self::buildMessageSets($topicPartitionMessages, $this->compressionCodec);
        $retryPolicy        = RetryPolicy::fromConfiguration($this->configuration);
        $result             = [];

        for ($attempt = 1;; $attempt++) {
            try {
                return self::mergeResult($result, $this->appendMessageSets($pendingMessageSets));
            } catch (TopicPartitionRequestException $exception) {
                // Whatever the broker did accept is kept, only the partitions that failed are sent again
                $result     = self::mergeResult($result, $exception->getPartialResult());
                $exceptions = $exception->getExceptions();

                $isLastAttempt = $attempt >= $retryPolicy->getMaxAttempts();
                if ($isLastAttempt || !RetryPolicy::isRetriable($exception)) {
                    throw new TopicPartitionRequestException($result, $exceptions);
                }

                $pendingMessageSets = self::onlyFailedPartitions($pendingMessageSets, $exceptions);
                // A retriable error almost always means that the leader of a partition moved
                $this->cluster->reload(array_keys($pendingMessageSets));
                $retryPolicy->backoff();
            }
        }
    }

    /**
     * Turns the records of every topic-partition into one compressed message set
     *
     * @param array<string, array<int, iterable<Record>>> $topicPartitionMessages Records per topic and partition
     * @param int                                         $compressionCodec       One of the {@see CompressionCodec}
     *
     * @return array<string, array<int, MessageSet>>
     */
    public static function buildMessageSets(array $topicPartitionMessages, int $compressionCodec): array
    {
        $messageSets = [];
        foreach ($topicPartitionMessages as $topic => $partitionMessages) {
            foreach ($partitionMessages as $partition => $records) {
                $messageSets[$topic][$partition] = MessageSet::fromRecords($records, $compressionCodec);
            }
        }

        return $messageSets;
    }

    /**
     * Sends one produce request to the leader of each partition of the batch and collects the answers
     *
     * @param array<string, array<int, MessageSet>> $topicPartitionMessageSets Message set of every partition
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     *
     * @throws TopicPartitionRequestException If at least one topic-partition could not be appended
     */
    private function appendMessageSets(array $topicPartitionMessageSets): array
    {
        $requiredAcks      = (int) $this->configuration[ProducerConfig::ACKS];
        $messageSetsByNode = [];
        $result            = [];
        $exceptions        = [];

        foreach ($topicPartitionMessageSets as $topic => $partitionMessageSets) {
            foreach ($partitionMessageSets as $partition => $messageSet) {
                try {
                    $leaderNode = $this->cluster->leaderFor($topic, $partition);
                } catch (KafkaException $exception) {
                    $exceptions[$topic][$partition] = $exception;

                    continue;
                }
                $messageSetsByNode[$leaderNode->nodeId][$topic][$partition] = $messageSet;
            }
        }

        foreach ($messageSetsByNode as $nodeId => $nodeTopicPartitionMessageSets) {
            try {
                $result = self::mergeResult(
                    $result,
                    $this->appendToNode($nodeId, $nodeTopicPartitionMessageSets, $requiredAcks)
                );
            } catch (TopicPartitionRequestException $nodeFailure) {
                // The broker answered, but reported an error for part of the partitions it was given
                $result = self::mergeResult($result, $nodeFailure->getPartialResult());
                foreach ($nodeFailure->getExceptions() as $topic => $partitionExceptions) {
                    foreach ($partitionExceptions as $partition => $partitionException) {
                        $exceptions[$topic][$partition] = $partitionException;
                    }
                }
            } catch (KafkaException $exception) {
                // The whole request to that broker failed, which makes every partition of it fail
                foreach ($nodeTopicPartitionMessageSets as $topic => $partitionMessageSets) {
                    foreach (array_keys($partitionMessageSets) as $partition) {
                        $exceptions[$topic][$partition] = $exception;
                    }
                }
            }
        }

        if ($exceptions !== []) {
            throw new TopicPartitionRequestException($result, $exceptions);
        }

        return $result;
    }

    /**
     * Appends the message sets that one broker leads and reads its answer
     *
     * @param array<string, array<int, MessageSet>> $nodeTopicPartitionMessageSets Message sets of that broker
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     *
     * @throws TopicPartitionRequestException If the broker reported an error for one of the partitions
     */
    private function appendToNode(int $nodeId, array $nodeTopicPartitionMessageSets, int $requiredAcks): array
    {
        $node = $this->cluster->nodeById($nodeId);
        if ($node === null) {
            throw new UnknownErrorException([
                'error'  => 'Can not find the node that leads these partitions',
                'nodeId' => $nodeId,
            ]);
        }

        $stream        = $node->getConnection($this->configuration);
        $correlationId = AbstractRequest::nextCorrelationId();
        $request       = new ProduceRequest(
            $nodeTopicPartitionMessageSets,
            $requiredAcks,
            (int) $this->configuration[ProducerConfig::TIMEOUT_MS],
            (string) $this->configuration[ProducerConfig::CLIENT_ID],
            $correlationId
        );
        $request->writeTo($stream);

        // `acks = 0` is the only request of the protocol that the broker does not answer at all
        if (!$request->expectsResponse()) {
            return [];
        }

        $response = ResponseValidator::read(ProduceResponse::class, $stream, $correlationId, ['nodeId' => $nodeId]);

        $result     = [];
        $exceptions = [];
        foreach ($response->topics as $topic => $topicResult) {
            /** @var ProduceResponsePartition $partitionInfo */
            foreach ($topicResult->partitions as $partitionId => $partitionInfo) {
                if ($partitionInfo->errorCode !== KafkaException::NO_ERROR) {
                    $exceptions[$topic][$partitionId] = KafkaException::fromCode(
                        $partitionInfo->errorCode,
                        ['topic' => $topic, 'partitionId' => $partitionId]
                    );

                    continue;
                }
                $result[$topic][$partitionId] = $partitionInfo;
            }
        }

        if ($exceptions !== []) {
            throw new TopicPartitionRequestException($result, $exceptions);
        }

        return $result;
    }

    /**
     * Keeps the message sets of the topic-partitions that failed, for the next attempt
     *
     * @param array<string, array<int, MessageSet>> $topicPartitionMessageSets Message sets that were sent
     * @param array<string, array<int, \Throwable>> $exceptions                Error of every failed partition
     *
     * @return array<string, array<int, MessageSet>>
     */
    private static function onlyFailedPartitions(array $topicPartitionMessageSets, array $exceptions): array
    {
        $pending = [];
        foreach ($exceptions as $topic => $partitionExceptions) {
            foreach (array_keys($partitionExceptions) as $partition) {
                if (isset($topicPartitionMessageSets[$topic][$partition])) {
                    $pending[$topic][$partition] = $topicPartitionMessageSets[$topic][$partition];
                }
            }
        }

        return $pending;
    }

    /**
     * Merges the accepted partitions of two answers
     *
     * @param array<string, array<int, ProduceResponsePartition>> $result
     * @param array<string, array<int, ProduceResponsePartition>> $addition
     *
     * @return array<string, array<int, ProduceResponsePartition>>
     */
    private static function mergeResult(array $result, array $addition): array
    {
        foreach ($addition as $topic => $partitions) {
            foreach ($partitions as $partition => $partitionResult) {
                $result[$topic][$partition] = $partitionResult;
            }
        }

        return $result;
    }
}
