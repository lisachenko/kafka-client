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
 * Metadata request of version 10 (key 3)
 *
 * The version of the **topic ids** (Kafka 2.8, KIP-516): every topic entry of the request carries a `topic_id`
 * in front of its name and the name itself is nullable, and every topic entry of the answer carries the id of
 * the topic, see {@see \Protocol\Kafka\Protocol\Data\MetadataRequestTopic} and
 * {@see \Protocol\Kafka\Common\TopicMetadata::$topicId}.
 *
 * It is also the **last version that carries `include_cluster_authorized_operations`**: version 11 (KIP-700)
 * moved the cluster-wide bitfield to the DescribeCluster api, so a caller that wants it from the Metadata api
 * asks with this class.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v11)" and "Topic ids (v10, KIP-516)"
 */
final class MetadataRequestV10 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
