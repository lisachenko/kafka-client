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

namespace Protocol\Kafka\Protocol\Request;

/**
 * Metadata request of version 9 (key 3)
 *
 * The first **flexible** version of the api (Kafka 2.4, KIP-482) and the last one whose topic entries are the
 * name alone: **version 10 (Kafka 2.8, KIP-516)** put a `topic_id` in front of every name, see
 * {@see \Protocol\Kafka\Protocol\Data\MetadataRequestTopic::$topicId}. The two booleans of KIP-430 are both
 * still on the wire here, which version 11 (KIP-700) is not, see {@see MetadataRequest}.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v11)" and "Topic ids (v10, KIP-516)"
 */
final class MetadataRequestV9 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
