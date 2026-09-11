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
 * Offsets (ListOffset) request of version 1 (Kafka 0.10.1), without the isolation level of version 2
 *
 * <pre>
 *   ListOffsets Request (Version: 1) => replica_id [topics]
 * </pre>
 *
 * Version 2 (KIP-98, Kafka 0.11) inserted `isolation_level` between the replica id and the topics, so this class
 * lowers the version constant that {@see OffsetsRequest::getScheme()} follows and the field is not written at all.
 * A 0.11.0.3 broker answers a request of this version as if `read_uncommitted` had been asked for, i.e. with the log
 * end offset rather than with the last stable offset.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v3), a.k.a. ListOffset"
 */
final class OffsetsRequestV1 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
