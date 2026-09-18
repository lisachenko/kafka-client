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
 * Produce request of version 10 (key 0)
 *
 * The leader discovery of KIP-951 (Kafka 3.7) and the last version below the `TransactionAbortable` of KIP-890:
 * the flexible frame of version 9 - the request header v2, a compact `transactional_id` and topic name, compact
 * arrays, a compact record set and a tagged-field section behind the body, every topic entry and every partition
 * entry - with the version 10 in its header.
 *
 * Version 11 (Kafka 3.8) sends this very body once more - `ProduceRequest.json` @ 3.8.1 declares no field of it
 * and its whole comment is "Version 11 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" - and
 * what it buys is the error code a transactional batch of it is refused with: **120**
 * ({@see \Protocol\Kafka\Common\Errors\TransactionAbortableException}) where this version is answered **48**
 * `InvalidTxnState`, see {@see ProduceRequest}.
 *
 * @see docs/protocol/3.9.md, sections "Produce API (key 0, v0 to v11)" and "The abortable transaction error of
 *      KIP-890 (v11)"
 */
final class ProduceRequestV10 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
