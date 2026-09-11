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
 * Metadata response of version 10 (key 3)
 *
 * The answer of the **topic ids** (Kafka 2.8, KIP-516): every topic entry carries the 16 raw bytes of its id
 * between the name and `is_internal`, see {@see \Protocol\Kafka\Common\TopicMetadata::$topicId}. It is also the
 * last answer that carries the `cluster_authorized_operations` bitfield of KIP-430, which version 11 (KIP-700)
 * moved to the DescribeCluster api, see {@see MetadataResponse}.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v11)" and "Topic ids (v10, KIP-516)"
 */
final class MetadataResponseV10 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
