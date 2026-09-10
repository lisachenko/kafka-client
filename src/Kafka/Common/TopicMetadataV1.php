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

namespace Protocol\Kafka\Common;

/**
 * Topic entry of a Metadata response of the versions 1 to 4
 *
 * <pre>
 *   TopicMetadata => TopicErrorCode TopicName IsInternal [PartitionMetadata]
 * </pre>
 *
 * `TOPIC_METADATA_V1` in `MetadataResponse.java` @ 1.1.1: version 1 of the api (Kafka 0.10.0) inserted the
 * `IsInternal` flag between the topic name and the partitions, and the versions 2, 3 and 4 answer with the very
 * same entry. What version 5 changed is not the topic entry either but the partition entries it holds, which now
 * carry `OfflineReplicas` ({@see PartitionMetadata::$offlineReplicas}), so this class only lowers the version
 * constant that {@see TopicMetadata::partitionClass()} follows.
 *
 * @see docs/protocol/1.1.md, section "Metadata API (key 3, v0 to v5)"
 */
final class TopicMetadataV1 extends TopicMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
