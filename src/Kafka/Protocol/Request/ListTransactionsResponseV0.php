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
 * ListTransactions answer of version 0 (Kafka 3.0), byte for byte the answer of version 1
 *
 * "Version 1 is the same as version 0 (KIP-994)" is the comment above the `validVersions` of
 * `ListTransactionsResponse.json` @ 3.8.1: the duration filter changed what a coordinator *selects*, not what it
 * writes back. The class exists because a version is a class on this line even when its schema is identical -
 * a frame recorded at version 0 is replayed through the class of version 0 - and because a caller that sends a
 * {@see ListTransactionsRequestV0} has to read its answer with the matching version.
 *
 * @see docs/protocol/4.3.md, section "ListTransactions API (key 66, v0 to v2)"
 */
final class ListTransactionsResponseV0 extends ListTransactionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
