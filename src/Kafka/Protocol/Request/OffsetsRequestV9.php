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
 * Offsets (ListOffset) request of version 9 (key 2)
 *
 * The version Kafka 3.9 added for the last tiered offset of KIP-1005 and the last one without a timeout: the flexible
 * frame of version 6, which ends with the topic array. `ListOffsetsRequest.json` @ 4.0.0 appends a `timeout_ms`
 * behind it with version 10 (KIP-1075), the time a broker may wait for a lookup in remote storage, see
 * {@see OffsetsRequest}. A request of this version carries none, which the broker reads as the default of the field,
 * **0**, and `ReplicaManager.fetchOffset` @ 4.0.0 replaces a timeout that is not above 0 with its own
 * `remote.list.offsets.request.timeout.ms` (30000 by default) - so a 4.x broker waits for a remote lookup of a
 * version 9 request as long as its configuration says.
 *
 * @see docs/protocol/4.3.md, sections "Offsets API (key 2, v0 to v11), a.k.a. ListOffset" and "The timeout of
 *      KIP-1075 (v10)"
 */
final class OffsetsRequestV9 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
