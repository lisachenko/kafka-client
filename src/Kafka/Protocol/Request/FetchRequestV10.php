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
 * Fetch request of version 10 (key 1)
 *
 * The frame of version 10 (Kafka 2.1, KIP-110) is the frame of version 9 - `FetchRequest.json` @ 2.8.2 has no
 * field of version 10 - and what it states is that the client understands a record batch compressed with zstd,
 * see {@see \Protocol\Kafka\Common\Record\CompressionCodec::ZSTD}. It is the last version **without** the
 * `rack_id` of KIP-392, which version 11 (Kafka 2.3) appended behind the forgotten topics, see
 * {@see FetchRequest::$rackId}: a consumer that sends this version is always served by the leader itself.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v12)" and "Version 10 and the zstd codec (KIP-110)"
 */
final class FetchRequestV10 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
