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
 * Metadata response object, version 3 (key 3, Kafka 0.11)
 *
 * <pre>
 *   Metadata Response (Version: 3) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
 * </pre>
 *
 * `METADATA_RESPONSE_V4 = METADATA_RESPONSE_V3` in `Protocol.java` @ 0.11.0.3: version 3 is where the leading
 * `throttle_time_ms` arrived, and version 4 - which only added `allow_auto_topic_creation` to the REQUEST
 * ({@see MetadataRequest}) - answers with the very same layout. This class therefore decodes the same bytes as
 * {@see MetadataResponse} and exists so that a version 3 request can be answered with a class of its own.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v5)"
 */
final class MetadataResponseV3 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
