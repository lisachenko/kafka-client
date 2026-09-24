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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\TopicPartition;

/**
 * Which partitions of one share group {@see AdminClient::listShareGroupOffsets()} asks about
 *
 * `ListShareGroupOffsetsSpec` of the Java admin client @ 4.3.1: a list of partitions, or `null` - the default - for
 * every partition the group holds state for, which goes out as the null topic array of DescribeShareGroupOffsets.
 *
 * @see docs/protocol/4.3.md, section "The share-group admin methods"
 */
final class ListShareGroupOffsetsSpec
{
    /**
     * Partitions to list, as topic => partition indexes; null for every partition the group holds state for
     *
     * @var array<string, list<int>>|null
     */
    public readonly ?array $topicPartitions;

    /**
     * @param array<string, list<int>>|iterable<TopicPartition>|null $topicPartitions Partitions to list, as
     *        topic => partition indexes or as {@see TopicPartition} objects; null for every partition of the group
     */
    public function __construct(?iterable $topicPartitions = null)
    {
        if ($topicPartitions === null) {
            $this->topicPartitions = null;

            return;
        }

        $normalized = [];
        foreach ($topicPartitions as $topic => $partitions) {
            if ($partitions instanceof TopicPartition) {
                $normalized[$partitions->topic][] = $partitions->partition;

                continue;
            }
            foreach ($partitions as $partition) {
                $normalized[(string) $topic][] = (int) $partition;
            }
        }
        $this->topicPartitions = $normalized;
    }

    /**
     * The spec of every partition the group holds state for, the `new ListShareGroupOffsetsSpec()` of Java
     */
    public static function allPartitions(): self
    {
        return new self(null);
    }
}
