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
 * Offsets (ListOffset) request of version 6 (key 2)
 *
 * The first **flexible** version of the api (Kafka 2.8, KIP-482) and the last one that only knows the two special
 * target times `-1` and `-2`: its frame is the frame of version 7, field for field and tag buffer for tag buffer -
 * `ListOffsetsRequest.json` @ 3.0.2 adds no field for version 7 and only comments "Version 7 enables listing
 * offsets by max timestamp (KIP-734)", see {@see OffsetsRequest}.
 *
 * What the version therefore decides is not how the bytes are written but **which target times the broker accepts**:
 * `KafkaApis.handleListOffsetRequestV1AndAbove` @ 3.9.2 holds the map `timestampMinSupportedVersion`, which demands
 * version **7** for {@see OffsetsRequest::MAX_TIMESTAMP} (`-3`), and answers a partition that asks for it below
 * that version with the error code **35** `UNSUPPORTED_VERSION` - per partition, with the timestamp and the offset
 * -1, not by closing the connection. The vectors `offsets.request.v6.max-timestamp` and
 * `offsets.response.v6.max-timestamp-unsupported` are that refusal on the node of this line.
 *
 * @see docs/protocol/4.3.md, section "Offsets API (key 2, v0 to v10), a.k.a. ListOffset"
 */
final class OffsetsRequestV6 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
