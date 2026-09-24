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
 * Topic entry of a Metadata answer of the versions 10 and 11
 *
 * The entry that carries the `topic_id` of KIP-516 (Kafka 2.8) and a topic name that is **never** null:
 * `MetadataResponse.json` @ 3.1.2 declares `"nullableVersions": "12+"` on that name, because only from version
 * 12 can a request name a topic by its id alone and only then can the broker fail to resolve one, see
 * {@see TopicMetadata}.
 *
 * @see docs/protocol/4.3.md, sections "Metadata API (key 3, v0 to v13)" and "Topic ids (v10, KIP-516)"
 */
final class TopicMetadataV10 extends TopicMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
