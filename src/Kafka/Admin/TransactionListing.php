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

namespace Protocol\Kafka\Admin;

/**
 * One transaction of a broker, as {@see AdminClient::listTransactions()} reports it
 *
 * `TransactionListing` of the Java admin client: the transactional id, the producer id behind it and the state it
 * is in - the three fields a ListTransactions (key 66) answer carries. Everything else about a transaction - the
 * epoch, the timeout, the start time and the partitions it touches - needs
 * {@see AdminClient::describeTransactions()}, which asks the coordinator of that id.
 *
 * @see docs/protocol/4.3.md, section "ListTransactions API (key 66, v0 and v1)"
 */
final class TransactionListing
{
    public function __construct(
        public readonly string $transactionalId,
        public readonly int $producerId,
        public readonly TransactionState $state
    ) {}
}
