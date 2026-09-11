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
 * The produce API, version 2
 *
 * <pre>
 *   ProduceRequest (Version: 2) => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
 *     RequiredAcks => int16
 *     Timeout      => int32
 * </pre>
 *
 * The body of a version 2 request is the body of a version 0 one - the `TransactionalId` that version 3 put in
 * front of `RequiredAcks` does not exist here - so this class only lowers the version constant, which drops that
 * field from {@see ProduceRequest::getScheme()}. This is the highest version that carries a **message set** of the
 * formats v0 and v1: everything the message format v2 added (headers, producer id, sequence numbers, transactions)
 * needs version 3.
 *
 * The **answer** of a version 2 request is the answer of a version 3 one, byte for byte, see
 * {@see ProduceResponseV2}: version 3 added no field to it.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v9)"
 */
final class ProduceRequestV2 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
