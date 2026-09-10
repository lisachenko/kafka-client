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

namespace Protocol\Kafka\Consumer\Internals;

use Protocol\Kafka\Protocol\Request\FetchMetadata;
use Protocol\Kafka\Protocol\Request\FetchRequest;

/**
 * Everything the next Fetch request to one broker consists of, as {@see FetchSessionHandler} worked it out.
 *
 * This is the `FetchSessionHandler.FetchRequestData` of the Java client (@ 1.1.1), field for field: the partitions
 * that go into the topics array of the request, the partitions that go into its `forgotten_topics_data`, the whole
 * set the fetch session holds afterwards, and the {@see FetchMetadata} the request travels with.
 *
 * The two partition maps are `topic => partition => fetch offset`, in the order the request asks for them - the
 * order the broker fills the answer of a bounded fetch in, see {@see FetchRequest::$maxBytes}. The Java client keeps
 * one flat `LinkedHashMap<TopicPartition, PartitionData>` for the same reason; the wire format groups the partitions
 * of a topic together, so a partition can only be ordered against the other partitions of its own topic here, which
 * is the very same approximation {@see \Protocol\Kafka\Consumer\KafkaConsumer} makes for its fetch order.
 *
 * @see docs/protocol/1.1.md, section "Fetch sessions (v7, KIP-227)"
 */
final class FetchRequestData
{
    /**
     * @param array<string, array<int, int>> $toSend            Partitions whose fetch offset the request states: all
     *                                                          of them in a full fetch, only the ones that changed
     *                                                          in an incremental one
     * @param array<string, list<int>>       $toForget          Partitions the session should drop, the
     *                                                          `forgotten_topics_data` of the request; always empty
     *                                                          in a full fetch, which states the whole set anyway
     * @param array<string, array<int, int>> $sessionPartitions Every partition the session holds after this request
     * @param FetchMetadata                  $metadata          Session id and epoch the request travels with
     */
    public function __construct(
        public readonly array $toSend,
        public readonly array $toForget,
        public readonly array $sessionPartitions,
        public readonly FetchMetadata $metadata,
    ) {}

    /**
     * Tells whether this request is a full fetch, i.e. one that carries every partition of the session
     */
    public function isFull(): bool
    {
        return $this->metadata->isFull();
    }

    /**
     * Returns the representation the Java client logs, e.g.
     * `IncrementalFetchRequest(toSend=(orders-0), toForget=(), implied=(orders-1))`
     */
    public function __toString(): string
    {
        if ($this->isFull()) {
            return 'FullFetchRequest(' . implode(', ', self::names($this->toSend)) . ')';
        }

        $implied = [];
        foreach ($this->sessionPartitions as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                if (!isset($this->toSend[$topic][$partition])) {
                    $implied[] = "{$topic}-{$partition}";
                }
            }
        }

        $forgotten = [];
        foreach ($this->toForget as $topic => $partitions) {
            foreach ($partitions as $partition) {
                $forgotten[] = "{$topic}-{$partition}";
            }
        }

        return 'IncrementalFetchRequest(toSend=(' . implode(', ', self::names($this->toSend)) . ')'
            . ', toForget=(' . implode(', ', $forgotten) . ')'
            . ', implied=(' . implode(', ', $implied) . '))';
    }

    /**
     * Renders a partition map as the `topic-partition` names of the Java client
     *
     * @param array<string, array<int, int>> $topicPartitions Partitions to name
     *
     * @return list<string>
     */
    private static function names(array $topicPartitions): array
    {
        $names = [];
        foreach ($topicPartitions as $topic => $partitions) {
            foreach (array_keys($partitions) as $partition) {
                $names[] = "{$topic}-{$partition}";
            }
        }

        return $names;
    }
}
