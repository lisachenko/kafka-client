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
 * AddPartitionsToTxn request of version 3 (Kafka 2.8, KIP-482): one transaction at the top level
 *
 * Version 4 (Kafka 3.5, KIP-890) moved the transactional id, the producer id, the epoch and the topics into a
 * `transactions` array and added the `verify_only` flag to each entry, and it made the api a **broker** api: a
 * 3.9.2 node authorizes it as `CLUSTER_ACTION` and answers a principal that is not a broker with the top-level
 * error code 31 ({@see AddPartitionsToTxnRequest}). This version is the last one a client may send, and the one
 * {@see \Protocol\Kafka\Client::addPartitionsToTxn()} does send.
 *
 * @see docs/protocol/3.9.md, section "AddPartitionsToTxn API (key 24, v0 to v5)"
 */
final class AddPartitionsToTxnRequestV3 extends AddPartitionsToTxnRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
