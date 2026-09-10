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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The replicas of one topic that live in one log directory, i.e. one entry of the `topics` array of a log directory
 *
 * <pre>
 *   DescribeLogDirsResponseTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => DescribeLogDirsResponsePartition
 * </pre>
 *
 * `DESCRIBE_LOG_DIRS_RESPONSE_V0` in `DescribeLogDirsResponse.schemaVersions()` @ 1.1.1. The answer is grouped by
 * directory first and by topic second, because a partition of a topic can sit in one directory while another
 * partition of the same topic sits in the next one - which is the whole point of the JBOD support of KIP-113 - and
 * a partition that is being moved appears in **both** directories at once, as the current and as the future log.
 *
 * Only the topics that really have a replica in this directory are listed: the broker builds the entry from the
 * logs it holds, not from the topics the request named.
 *
 * @see docs/protocol/1.1.md, section "DescribeLogDirs API (key 35, v0)"
 */
class DescribeLogDirsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this entry belongs to
     */
    public string $topic;

    /**
     * Replicas of this topic in this log directory, indexed by the partition id
     *
     * @var array<int, DescribeLogDirsResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => DescribeLogDirsResponsePartition::class],
        ];
    }
}
