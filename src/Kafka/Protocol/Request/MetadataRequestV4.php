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
 * Metadata request of version 4 (Kafka 0.11, KIP-4)
 *
 * <pre>
 *   Metadata Request (Version: 4) => [topics] allow_auto_topic_creation
 *     topics                    => NULLABLE_ARRAY of STRING
 *     allow_auto_topic_creation => BOOLEAN
 * </pre>
 *
 * `METADATA_REQUEST_V5` is `METADATA_REQUEST_V4` in `MetadataRequest.schemaVersions()` @ 1.1.1, so the frame of
 * this class differs from the one of {@see MetadataRequest} in the version field of the header alone; what version
 * 5 changed is the ANSWER, whose partition entries gained `offline_replicas` ({@see MetadataResponseV4} for the
 * frame without it). This class only lowers the version constant that {@see MetadataResponse::topicClass()}
 * follows on the answering side.
 *
 * @see docs/protocol/1.1.md, section "Metadata API (key 3, v0 to v5)"
 */
final class MetadataRequestV4 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
