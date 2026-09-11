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
 * The produce API, version 3
 *
 * <pre>
 *   ProduceRequest (Version: 3) => TransactionalId RequiredAcks Timeout [TopicName [Partition RecordSetSize
 *                                                                                   RecordSet]]
 *     TransactionalId => nullable string
 * </pre>
 *
 * Version 3 (Kafka 0.11.0, KIP-98) is the first version that prefixes the body with the nullable
 * `TransactionalId` of the producer and the first one whose record set is a **record batch of the message format
 * v2**; the versions 4 and 5 send the very same body. A version 3 client does not understand the error code 56
 * `KAFKA_STORAGE_ERROR` - a broker answers it with 6 `NOT_LEADER_FOR_PARTITION` - and its answer has no
 * `LogStartOffset`, so this class only lowers the version constant that {@see ProduceRequest::getScheme()} and
 * {@see ProduceResponse::topicClass()} follow.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v9)"
 */
final class ProduceRequestV3 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
