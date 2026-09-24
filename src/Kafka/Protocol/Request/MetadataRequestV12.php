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
 * Metadata request of version 12 (key 3)
 *
 * The version at which the `topic_id` of a request entry really works (Kafka 3.1, KIP-516), and the last one whose
 * answer has no top-level error code. Its body is byte for byte the one of version 13 - `MetadataRequest.json` @
 * 4.0.0 adds no field with version 13 and only comments "Version 13 supports top-level error code in the response" -
 * so the version decides which answer the broker writes: {@see MetadataResponseV12}, which ends with the topic array,
 * where version 13 appends the `error_code` of KIP-1102 behind it, see {@see MetadataRequest}.
 *
 * @see docs/protocol/4.3.md, sections "Metadata API (key 3, v0 to v13)" and "Metadata by topic id (v12, KIP-516)"
 */
final class MetadataRequestV12 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
