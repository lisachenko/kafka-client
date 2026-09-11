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
 * The produce API, version 4
 *
 * <pre>
 *   ProduceRequest (Version: 4) => TransactionalId RequiredAcks Timeout [TopicName [Partition RecordSetSize
 *                                                                                   RecordSet]]
 * </pre>
 *
 * `PRODUCE_REQUEST_V4` is `PRODUCE_REQUEST_V3` in `ProduceRequest.schemaVersions()` @ 1.1.1: the body of a version 4
 * request is the body of a version 3 one, byte for byte, and the version alone states that the client understands
 * the error code **56** `KAFKA_STORAGE_ERROR` (Kafka 1.0). A broker that would report 56 answers a request of
 * version 3 or lower with **6** `NOT_LEADER_FOR_PARTITION` instead. What version 4 does *not* carry is the
 * `LogStartOffset` that version 5 added to every partition of the answer, so this class only lowers the version
 * constant that {@see ProduceResponse::topicClass()} follows on the answering side.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v7)"
 */
final class ProduceRequestV4 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
