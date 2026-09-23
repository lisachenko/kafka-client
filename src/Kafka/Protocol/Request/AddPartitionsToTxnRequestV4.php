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
 * AddPartitionsToTxn request of version 4 (Kafka 3.5, KIP-890), the batched frame before the code 120
 *
 * "Version 5 adds support for new error code TRANSACTION_ABORTABLE" (`AddPartitionsToTxnRequest.json` @ 3.8.1):
 * the `transactions` array of the version 4 is written byte for byte the same way, and the version alone says
 * whether the **broker** that sends it understands the 120. The version 4 also carried
 * `"latestVersionUnstable": true` at the 3.5.2 tag and lost the flag with the version 5, which is why a 3.9.2
 * node announces the whole range 0 to 5 for the key 24 without `unstable.api.versions.enable`.
 *
 * Neither version is sent by this client: {@see \Protocol\Kafka\Client::addPartitionsToTxn()} keeps the
 * {@see AddPartitionsToTxnRequestV3} of Kafka 2.8, because a 3.9.2 node authorizes every version from 4 on as
 * `CLUSTER_ACTION` and answers a principal that is not a broker with the top-level error code 31
 * ({@see AddPartitionsToTxnRequest}).
 *
 * @see docs/protocol/4.3.md, section "AddPartitionsToTxn API (key 24, v0 to v5)"
 */
final class AddPartitionsToTxnRequestV4 extends AddPartitionsToTxnRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
