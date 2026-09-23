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

namespace Protocol\Kafka\Tests\Fixture;

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;

/**
 * Waits until every partition of a topic has a leader that serves it.
 *
 * Asking for the metadata of an unknown topic creates it when `auto.create.topics.enable` is set, but the answer to
 * that very first request announces the topic with error 5 (LeaderNotAvailable) and no partitions at all: the
 * controller elects the leaders afterwards, so a client has to ask again.
 *
 * A leader in the metadata is not yet a leader that answers. A KRaft node publishes one metadata delta to its
 * metadata cache first and to its replica manager after it, so between the two a Metadata answer names the leader
 * of a fresh partition while a Produce, a Fetch or a ListOffsets to that very broker is still answered with **6**
 * `NotLeaderForPartition` (or 3 while the partition is not there at all). The gap is the replay lag of the node -
 * tens of milliseconds on an idle node, seconds on a loaded CI runner - so once the metadata is complete the probe
 * asks the broker it is connected to for the latest offset of every partition and returns only when every one of
 * them is answered with the code 0. The probe therefore has to be connected to the leader of the partitions, which
 * the one-node cluster of the integration suite guarantees; `$awaitServingLeaders = false` keeps the metadata-only
 * behaviour for a cluster where it is not.
 *
 * @see docs/protocol/4.3.md, section "Metadata API (key 3, v0 to v13)"
 * @see docs/protocol/4.3.md, section "Cluster readiness"
 */
final class TopicMetadataProbe
{
    /**
     * The codes a partition answers while its broker has not applied the metadata delta that made it the leader
     */
    private const array NOT_YET_SERVED = [
        KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
        KafkaException::LEADER_NOT_AVAILABLE,
        KafkaException::NOT_LEADER_FOR_PARTITION,
        KafkaException::REPLICA_NOT_AVAILABLE,
    ];

    /**
     * Number of requests - Metadata and ListOffsets - that the last call needed
     */
    private int $attempts = 0;

    /**
     * @param \Closure(): Stream $streamFactory       Opens a fresh connection to any broker of the cluster
     * @param float              $timeout             How long to keep asking, in seconds
     * @param string             $clientId            Client id to send along with the requests
     * @param int                $backoffMicroseconds How long to wait between two attempts
     * @param bool               $awaitServingLeaders Whether to wait until the broker the stream reaches answers a
     *                                                ListOffsets of every partition with the code 0, and not only
     *                                                until the metadata names a leader
     */
    public function __construct(
        private readonly \Closure $streamFactory,
        private readonly float $timeout = 30.0,
        private readonly string $clientId = 'kafka-client-probe',
        private readonly int $backoffMicroseconds = 250000,
        private readonly bool $awaitServingLeaders = true,
    ) {}

    /**
     * Returns the metadata of the topic as soon as each of its partitions has an elected leader that serves it
     */
    public function awaitTopicWithLeaders(string $topic): TopicMetadata
    {
        $deadline       = microtime(true) + $this->timeout;
        $this->attempts = 0;

        do {
            $this->attempts++;
            $stream = ($this->streamFactory)();
            new MetadataRequest([$topic], true, $this->clientId, $this->attempts)->writeTo($stream);

            $topicMetadata = MetadataResponse::unpack($stream)->topics[$topic] ?? null;
            if ($topicMetadata !== null && $topicMetadata->topicErrorCode === 0 && $topicMetadata->partitions !== []) {
                $leaderless = array_filter(
                    $topicMetadata->partitions,
                    static fn(PartitionMetadata $partition): bool => $partition->leader < 0
                );
                if ($leaderless === [] && (!$this->awaitServingLeaders || $this->leadersServe($topic, $topicMetadata))) {
                    return $topicMetadata;
                }
            }
            usleep($this->backoffMicroseconds);
        } while (microtime(true) < $deadline);

        throw new \RuntimeException(
            "No leader was elected for the partitions of {$topic} within {$this->timeout} seconds"
        );
    }

    /**
     * Tells whether the broker the stream factory reaches answers the latest offset of every partition of the topic
     *
     * The codes 3, 5, 6 and 9 are the ones a partition answers while the broker has not applied the metadata delta
     * that made it the leader; any other refusal is not a matter of waiting and is thrown as it is.
     *
     * @throws KafkaException If a partition is refused with a code that a wait does not cure
     */
    private function leadersServe(string $topic, TopicMetadata $topicMetadata): bool
    {
        $partitions = [];
        foreach ($topicMetadata->partitions as $partition) {
            $partitions[$partition->partitionId] = OffsetsRequest::LATEST;
        }

        $stream = ($this->streamFactory)();
        $this->attempts++;
        new OffsetsRequest(
            [$topic => $partitions],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_UNCOMMITTED,
            $this->clientId,
            $this->attempts
        )->writeTo($stream);

        $answer = OffsetsResponse::unpack($stream)->topics[$topic] ?? null;
        foreach ($partitions as $partitionId => $unused) {
            $errorCode = $answer?->partitions[$partitionId]?->errorCode ?? KafkaException::UNKNOWN_TOPIC_OR_PARTITION;
            if ($errorCode === KafkaException::NO_ERROR) {
                continue;
            }
            if (in_array($errorCode, self::NOT_YET_SERVED, true)) {
                return false;
            }

            throw KafkaException::fromCode(
                $errorCode,
                ['topic' => $topic, 'partitionId' => $partitionId]
            );
        }

        return true;
    }

    /**
     * Returns how many requests - Metadata and ListOffsets - the last call needed
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }
}
