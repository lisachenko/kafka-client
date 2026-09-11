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

use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

/**
 * Waits until every partition of a topic has a leader.
 *
 * Asking for the metadata of an unknown topic creates it when `auto.create.topics.enable` is set, but the answer to
 * that very first request announces the topic with error 5 (LeaderNotAvailable) and no partitions at all: the
 * controller elects the leaders afterwards, so a client has to ask again.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
final class TopicMetadataProbe
{
    /**
     * Number of Metadata requests that the last call needed
     */
    private int $attempts = 0;

    /**
     * @param \Closure(): Stream $streamFactory       Opens a fresh connection to any broker of the cluster
     * @param float              $timeout             How long to keep asking, in seconds
     * @param string             $clientId            Client id to send along with the requests
     * @param int                $backoffMicroseconds How long to wait between two attempts
     */
    public function __construct(
        private readonly \Closure $streamFactory,
        private readonly float $timeout = 30.0,
        private readonly string $clientId = 'kafka-client-probe',
        private readonly int $backoffMicroseconds = 250000,
    ) {}

    /**
     * Returns the metadata of the topic as soon as each of its partitions has an elected leader
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
                if ($leaderless === []) {
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
     * Returns how many Metadata requests the last call needed
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }
}
