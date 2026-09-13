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
 * Fetch API (key 1), version 6
 *
 * <pre>
 *   FetchRequest (Version: 6) => ReplicaId MaxWaitTime MinBytes MaxBytes IsolationLevel
 *                                [TopicName [Partition FetchOffset LogStartOffset MaxBytes]]
 * </pre>
 *
 * `FETCH_REQUEST_V6` is `FETCH_REQUEST_V5` in `FetchRequest.schemaVersions()` @ 1.1.1 and the answer of version 6
 * is the answer of version 5, byte for byte: version 6 (Kafka 1.0) states one thing about the *client*, that it
 * understands the error code **56** `KAFKA_STORAGE_ERROR`. A broker that would report 56 answers a request of
 * version 5 or lower with 6 `NOT_LEADER_FOR_PARTITION` instead.
 *
 * What version 6 does not carry is the `SessionId`, the `Epoch` and the `forgotten_topics_data` of the incremental
 * fetch sessions that version 7 added (KIP-227), so this class only lowers the version constant that
 * {@see FetchRequest::getScheme()} follows.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchRequestV6 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
