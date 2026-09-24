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
 * ListTransactions request of version 1 (Kafka 3.8, KIP-994): the duration filter, without the id pattern
 *
 * The frame of {@see ListTransactionsRequest} minus its last field: version 2 (Kafka 4.1, KIP-1152) appended the
 * nullable `transactional_id_pattern`, and this is the request below it. The
 * {@see ListTransactionsRequest::$transactionalIdPattern} of an instance of this class never reaches the wire, and
 * the coordinator lists every transactional id the other three filters let through.
 *
 * @see docs/protocol/4.3.md, sections "ListTransactions API (key 66, v0 to v2)" and "The transactional id pattern
 *      of KIP-1152 (v2)"
 */
final class ListTransactionsRequestV1 extends ListTransactionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
