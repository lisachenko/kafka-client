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
 * Metadata response of version 12 (key 3)
 *
 * The answer KIP-516 (Kafka 3.1) gave a nullable topic name, for the entry of a topic id the cluster could not
 * resolve. It differs from the version 13 answer in one field only: it ends with the topic array, where
 * `MetadataResponse.json` @ 4.0.0 appends the top-level `error_code` of KIP-1102 behind it, see
 * {@see MetadataResponse::$errorCode}.
 *
 * @see docs/protocol/4.3.md, sections "Metadata API (key 3, v0 to v13)" and "Metadata by topic id (v12, KIP-516)"
 */
final class MetadataResponseV12 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
