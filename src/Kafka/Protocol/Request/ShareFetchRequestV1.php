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
 * ShareFetch request of version 1 (Kafka 4.1, KIP-932): the frame without the acquire mode and the renew flag
 *
 * The stable version of Kafka 4.1 - `ShareFetchRequest.json` @ 4.1.0, `validVersions` `1` - whose `max_records` is
 * always the batch-optimized limit and whose acknowledgements know the types 0 to 3 only. Version 2 (Kafka 4.2,
 * KIP-1206 and KIP-1222) added `share_acquire_mode` and `is_renew_ack` behind `batch_size` ({@see ShareFetchRequest});
 * the two arguments of the constructor are not written by this version, as `ShareFetchRequest.Builder.build()` @
 * 4.2.0 refuses to send anything but the batch-optimized mode without a renew at version 1.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1 and v2)"
 */
final class ShareFetchRequestV1 extends ShareFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
