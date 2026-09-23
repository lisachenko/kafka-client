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
 * Offsets (ListOffset) request of version 7 (key 2)
 *
 * The version Kafka 3.0 added for the max timestamp of KIP-734 and the last one that does not know the local log
 * start offset: its frame is the frame of version 8, field for field and tag buffer for tag buffer -
 * `ListOffsetsRequest.json` @ 3.5.2 adds no field for version 8 and only comments "Version 8 enables listing
 * offsets by local log start offset (KIP-405)", see {@see OffsetsRequest}.
 *
 * What the version decides is therefore not how the bytes are written but **which target times the broker
 * accepts**: `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 demands version **8** for
 * {@see OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP} (`-4`) in its `timestampMinSupportedVersion` map and answers a
 * partition that asks for it below that version with the error code **35** `UNSUPPORTED_VERSION` - per partition,
 * with the timestamp and the offset -1, not by closing the connection. The vectors
 * `offsets.request.v7.earliest-local` and `offsets.response.v7.earliest-local-unsupported` are that refusal on
 * the node of this line.
 *
 * @see docs/protocol/4.3.md, section "Offsets API (key 2, v0 to v10), a.k.a. ListOffset"
 */
final class OffsetsRequestV7 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
