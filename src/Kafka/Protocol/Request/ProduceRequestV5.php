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
 * The produce API, version 5
 *
 * <pre>
 *   ProduceRequest (Version: 5) => TransactionalId RequiredAcks Timeout [TopicName [Partition RecordSetSize
 *                                                                                   RecordSet]]
 * </pre>
 *
 * `PRODUCE_REQUEST_V5` is `PRODUCE_REQUEST_V3` - `ProduceRequest.json` @ 2.8.2 has no field above version 3 - so
 * the body of a version 5 request is the body of a version 3 one, byte for byte. Version 5 (Kafka 1.0) states that
 * the client understands the `LogStartOffset` that the answer gained, see
 * {@see \Protocol\Kafka\Protocol\Data\ProduceResponsePartition::$logStartOffset}.
 *
 * This is the highest version a **Kafka 1.1.1** broker serves and the version the 1.x line of this client sent,
 * and it is the last version that promises nothing about KIP-219. A 2.8.2 broker still serves it - and, measured,
 * throttles it exactly as it throttles version 6, by answering first and muting the channel, see
 * {@see ProduceRequest}: a client that sends version 5 does not get the old behaviour back, it only fails to
 * announce that it understands the new one.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v9)"
 */
final class ProduceRequestV5 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
