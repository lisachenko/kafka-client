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
 * Metadata response object, version 4 (key 3, Kafka 0.11)
 *
 * <pre>
 *   Metadata Response (Version: 4) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
 *     partition_metadata => partition_error_code partition_id leader [replicas] [isr]
 * </pre>
 *
 * `METADATA_RESPONSE_V4` is `METADATA_RESPONSE_V3` in `MetadataResponse.schemaVersions()` @ 1.1.1: what version 4
 * added, `allow_auto_topic_creation`, is a field of the REQUEST ({@see MetadataRequest}), and the answer of the
 * versions 3 and 4 is the same frame. The `offline_replicas` that version 5 (Kafka 1.0, KIP-112/113) appended to
 * every partition entry is not on the wire here, so this class lowers the version constant that
 * {@see MetadataResponse::topicClass()} and, through it,
 * {@see \Protocol\Kafka\Common\TopicMetadata::partitionClass()} follow.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v8)"
 */
final class MetadataResponseV4 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
