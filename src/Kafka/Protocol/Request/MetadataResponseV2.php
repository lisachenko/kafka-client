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
 * Metadata response object, version 2 (key 3, Kafka 0.10.1)
 *
 * <pre>
 *   Metadata Response (Version: 2) => [brokers] cluster_id controller_id [topic_metadata]
 * </pre>
 *
 * The answer of Kafka 0.10.1: the `ClusterId` of version 2 is there, the leading `throttle_time_ms` that version 3
 * added is not, so this class only lowers the version constant that {@see MetadataResponse::getScheme()} follows.
 * Reading such an answer with the version 3 class would take the broker count for a throttle time.
 *
 * @see docs/protocol/1.1.md, section "Metadata API (key 3, v0 to v5)"
 */
final class MetadataResponseV2 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
