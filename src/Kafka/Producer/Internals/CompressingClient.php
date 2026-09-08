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
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;

/**
 * The produce path of a {@see KafkaProducer} that was configured with a `compression.type` other than `none`.
 *
 * A compressed batch is a message set of exactly one message, whose value is the whole batch compressed with the
 * configured codec and whose attributes announce that codec; {@see MessageSet::fromRecords()} builds it, and the
 * broker unwraps it on append and hands it back to a consumer as the records it holds.
 *
 * This class only exists because {@see Client::produce()} always builds *uncompressed* message sets out of the
 * records it is given: it can not be told which codec to use. It is meant to be deleted as soon as the client
 * applies the codec itself, which is the one-line change described in the pull request of this producer, and the
 * whole producer goes back to a single produce path.
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
     * @return array<string, array<int, ProduceResponsePartition>> Accepted partitions, empty for `acks = 0`
     *
     * @throws TopicPartitionRequestException when part of the topic-partitions of the request failed
     */
    public function produce(array $topicPartitionMessages): array
    {
        $requiredAcks = (int) $this->configuration[ProducerConfig::ACKS];
        $messageSets  = self::buildMessageSets($topicPartitionMessages, $this->compressionCodec);

        // Every message set goes to the leader of its partition, so one request is built per broker
        $messageSetsByNode = [];
        foreach ($messageSets as $topic => $partitionMessageSets) {
            foreach ($partitionMessageSets as $partition => $messageSet) {
                $leaderNode = $this->cluster->leaderFor($topic, $partition);

                $messageSetsByNode[$leaderNode->nodeId][$topic][$partition] = $messageSet;
            }
        }

        $result     = [];
        $exceptions = [];
        foreach ($messageSetsByNode as $nodeId => $nodeTopicPartitionMessageSets) {
            $stream  = $this->cluster->nodeById($nodeId)->getConnection($this->configuration);
            $request = new ProduceRequest(
                $nodeTopicPartitionMessageSets,
                $requiredAcks,
                (int) $this->configuration[ProducerConfig::TIMEOUT_MS],
                $this->configuration[ProducerConfig::CLIENT_ID]
            );
            $request->writeTo($stream);

            // `acks = 0` is the only request of the protocol that the broker does not answer at all
            if (!$request->expectsResponse()) {
                continue;
            }

            $response = ProduceResponse::unpack($stream);
            foreach ($response->topics as $topic => $topicResult) {
                /** @var ProduceResponsePartition $partitionInfo */
                foreach ($topicResult->partitions as $partitionId => $partitionInfo) {
                    if ($partitionInfo->errorCode !== 0) {
                        $exceptions[$topic][$partitionId] = KafkaException::fromCode(
                            $partitionInfo->errorCode,
                            ['topic' => $topic, 'partitionId' => $partitionId]
                        );

                        continue;
                    }
                    $result[$topic][$partitionId] = $partitionInfo;
                }
            }
        }

        if ($exceptions !== []) {
            throw new TopicPartitionRequestException($result, $exceptions);
        }

        return $result;
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
}
