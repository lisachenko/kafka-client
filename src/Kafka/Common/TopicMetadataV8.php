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
 * Topic entry of a Metadata answer of the versions 8 and 9
 *
 * The entry of version 8 (Kafka 2.3, KIP-430): the error code, the name, `is_internal`, the partitions and the
 * `topic_authorized_operations` bitfield. **Version 10 (Kafka 2.8, KIP-516)** inserted the `topic_id` between
 * the name and `is_internal`, see {@see TopicMetadata::$topicId}; this class is the entry without it.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
final class TopicMetadataV8 extends TopicMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
