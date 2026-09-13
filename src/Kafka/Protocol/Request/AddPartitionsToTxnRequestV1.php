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
 * AddPartitionsToTxn request of version 1, the frame of version 2 with a lower version field
 *
 * "Version 2 adds the support for new error code PRODUCER_FENCED" (`AddPartitionsToTxnRequest.json` @ 2.7.2): the version
 * Kafka 2.7 added with KIP-588 changes no byte of the request. The api is **not** flexible in this version -
 * `"flexibleVersions": "none"` at the 2.7.2 tag - and only became one with the version 3 of Kafka 2.8.
 *
 * @see docs/protocol/2.8.md, section "AddPartitionsToTxn API (key 24, v0 to v3)"
 */
final class AddPartitionsToTxnRequestV1 extends AddPartitionsToTxnRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
