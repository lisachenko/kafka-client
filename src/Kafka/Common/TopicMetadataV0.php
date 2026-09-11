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
 * Topic entry of a Metadata response of version 0, i.e. the one without the `IsInternal` flag
 *
 * <pre>
 *   TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
 * </pre>
 *
 * `TOPIC_METADATA_V0` in `MetadataResponse.java` @ 1.1.1. The class exists only to lower the version constant that
 * {@see TopicMetadata::getScheme()} follows; the `isInternal` property of the parent stays null for it, which is
 * "the answer did not say", not "the topic is not internal".
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v5)"
 */
final class TopicMetadataV0 extends TopicMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
