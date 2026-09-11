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
 * Metadata response object, version 5 (key 3)
 *
 * <pre>
 *   Metadata Response (Version: 5) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
 * </pre>
 *
 * The frame of version 5 (Kafka 1.0, KIP-112/113) is the frame of version 6, byte for byte - every partition entry
 * ends with the `offline_replicas` array of version 5, and nothing of version 6 is on the wire. The two versions
 * differ only in what the client promises about the throttle time of KIP-219, see {@see MetadataResponse}.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v9)" and "Cluster readiness"
 */
final class MetadataResponseV5 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
