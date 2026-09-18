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
 * Offsets (ListOffset) request of version 8 (key 2)
 *
 * The version Kafka 3.5 added for the local log start offset of KIP-405 and the last one that does not know the
 * last tiered offset: its frame is the frame of version 9, field for field and tag buffer for tag buffer -
 * `ListOffsetsRequest.json` @ 3.9.2 adds no field for version 9 and only comments "Version 9 enables listing
 * offsets by last tiered offset (KIP-1005)", see {@see OffsetsRequest}.
 *
 * What the version decides is therefore not how the bytes are written but **which target times the broker
 * accepts**: `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 demands version **9** for
 * {@see OffsetsRequest::LATEST_TIERED_TIMESTAMP} (`-5`) in its `timestampMinSupportedVersion` map and answers a
 * partition that asks for it below that version with the error code **35** `UNSUPPORTED_VERSION` - per partition,
 * with the timestamp and the offset -1, not by closing the connection. The vectors
 * `offsets.request.v8.latest-tiered` and `offsets.response.v8.latest-tiered-unsupported` are that refusal on the
 * node of this line.
 *
 * @see docs/protocol/3.9.md, section "Offsets API (key 2, v0 to v9), a.k.a. ListOffset"
 */
final class OffsetsRequestV8 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
