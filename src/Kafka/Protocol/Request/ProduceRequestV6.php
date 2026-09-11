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
 * The produce API, version 6
 *
 * <pre>
 *   ProduceRequest (Version: 6) => TransactionalId RequiredAcks Timeout [TopicName [Partition RecordSetSize
 *                                                                                   RecordSet]]
 * </pre>
 *
 * `PRODUCE_REQUEST_V6` is `PRODUCE_REQUEST_V3` - `ProduceRequest.json` @ 2.8.2 has no field above version 3 - so
 * the body of a version 6 request is the body of a version 3 one, byte for byte. Version 6 (Kafka 2.0, KIP-219)
 * states that the client waits out the `ThrottleTime` of the answer itself, see {@see ProduceRequest}.
 *
 * This is the version the Kafka 2.0 part of this line sent, and the last one that may **not** carry a
 * zstd-compressed record batch: `ProduceRequest.validateRecords` @ 2.8.2 refuses a batch whose compression type is
 * `ZSTD` below version 7 with `UnsupportedCompressionTypeException`, the code **76**. Version 7 is what
 * {@see ProduceRequest} sends, see {@see \Protocol\Kafka\Common\Record\CompressionCodec::ZSTD}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v7)"
 */
final class ProduceRequestV6 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
