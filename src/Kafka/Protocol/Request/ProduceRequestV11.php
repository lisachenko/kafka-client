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
 * Produce request of version 11 (key 0)
 *
 * The abortable transaction error of KIP-890 (Kafka 3.8) and the last version below the transaction protocol v2: the
 * flexible frame of version 9 - the request header v2, a compact `transactional_id` and topic name, compact arrays,
 * a compact record set and a tagged-field section behind the body, every topic entry and every partition entry -
 * with the version 11 in its header.
 *
 * Version 12 (Kafka 4.0) sends this very body once more - `ProduceRequest.json` @ 4.0.0 declares no field of it and
 * comments "Version 12 is the same as version 11 (KIP-890)" - and what it changes is what a **transactional** batch
 * of it means to a node that finalizes `transaction.version` 2: the broker adds the partition to the transaction
 * itself, the AddPartitionsToTxn round trip of the protocol v1 is gone. "If V2 is disabled, the client can't use
 * produce request version higher than 11 within a transaction", which is why this version stays the one a
 * transaction of the protocol v1 is written with, see {@see \Protocol\Kafka\Client::produceVersion()}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v12)" and "The transaction protocol v2 of KIP-890
 *      part 2 (v12)"
 */
final class ProduceRequestV11 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 11;
}
