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
 * Offsets (ListOffset) request of version 10 (key 2)
 *
 * The version Kafka 4.0 added for the `timeout_ms` of KIP-1075, and the frame of version 11 byte for byte:
 * `ListOffsetsRequest.json` @ 4.2.0 comments "Version 11 enables listing offsets by earliest pending upload offset
 * (KIP-1023)" and declares not a field of it, see {@see OffsetsRequest}. What the version 11 buys is the special
 * target time {@see OffsetsRequest::EARLIEST_PENDING_UPLOAD_TIMESTAMP} (`-6`): `ReplicaManager` @ 4.2.0 demands
 * version 11 for it in its `timestampMinSupportedVersion`, so a request of this version that asks for `-6` is
 * answered **35** `UNSUPPORTED_VERSION` for that partition, with the timestamp, the offset and the leader epoch -1
 * and the connection untouched - the same per-partition refusal the target times `-3` to `-5` get from the versions
 * below the one that introduced them.
 *
 * @see docs/protocol/4.3.md, sections "Offsets API (key 2, v0 to v11), a.k.a. ListOffset", "The timeout of
 *      KIP-1075 (v10)" and "The earliest pending upload offset of KIP-1023 (v11)"
 */
final class OffsetsRequestV10 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
