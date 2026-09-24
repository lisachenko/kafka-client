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
 * EndTxn request of version 4, the frame of version 5 with a lower version field
 *
 * "Version 5 enables bumping epoch on every transaction (KIP-890 Part 2)" (`EndTxnRequest.json` @ 4.0.0): the
 * transactional id, the producer id, the epoch and the one boolean of the result, unchanged since Kafka 0.11 and
 * written in the flexible encoding since the version 3. The version 4 of Kafka 3.8 ends a transaction of the
 * protocol **v1** - one whose partitions an {@see AddPartitionsToTxnRequest} enrolled - and it is the version the
 * Java `EndTxnRequest.Builder` @ 4.0.0 caps at (`LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`) on a cluster that does
 * not finalize `transaction.version` 2, and the highest one a 3.x node serves.
 *
 * @see docs/protocol/4.3.md, section "EndTxn API (key 26, v0 to v5)"
 */
final class EndTxnRequestV4 extends EndTxnRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
