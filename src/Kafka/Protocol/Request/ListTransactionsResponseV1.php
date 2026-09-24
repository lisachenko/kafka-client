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
 * ListTransactions answer of version 1 (Kafka 3.8, KIP-994), byte for byte the answer of versions 0 and 2
 *
 * Neither the `duration_filter` of version 1 nor the `transactional_id_pattern` of version 2 changed what a
 * coordinator writes back. The class exists because a version is a class on this line even when its schema is
 * identical: a frame recorded at version 1 is replayed through the class of version 1, and a caller that sends a
 * {@see ListTransactionsRequestV1} reads its answer with it.
 *
 * @see docs/protocol/4.3.md, sections "ListTransactions API (key 66, v0 to v2)" and "The transactional id pattern
 *      of KIP-1152 (v2)"
 */
final class ListTransactionsResponseV1 extends ListTransactionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
