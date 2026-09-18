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
 * Metadata request of version 11 (key 3)
 *
 * The version KIP-700 (Kafka 2.8) created by taking `include_cluster_authorized_operations` out of the version 10
 * frame. Its body is byte for byte the one of version 12 - `MetadataRequest.json` @ 3.1.2 adds no field with
 * version 12 - and the difference is entirely on the server: until version 12 the `topic_id` of a topic entry is
 * ignored ("Versions 10 and 11 should not use the topicId field or set topic name to null"), so a request of this
 * version names its topics by name, see {@see MetadataRequest}.
 *
 * @see docs/protocol/3.9.md, sections "Metadata API (key 3, v0 to v12)" and "Metadata by topic id (v12, KIP-516)"
 */
final class MetadataRequestV11 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 11;
}
