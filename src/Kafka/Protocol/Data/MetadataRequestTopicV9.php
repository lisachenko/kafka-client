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

/**
 * One topic a Metadata request of version 9 asks about
 *
 * The entry of the first flexible version (Kafka 2.4): the topic name and the tagged-field section that closes
 * every structure of such a version, and nothing else. **Version 10 (Kafka 2.8, KIP-516)** put a `topic_id` in
 * front of the name and made the name nullable, see {@see MetadataRequestTopic::$topicId}.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
final class MetadataRequestTopicV9 extends MetadataRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
