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
 * Fetch response of version 10 (key 1)
 *
 * The frame of the versions 7 to 10, unchanged: the throttle time, the session error code, the session id and the
 * partitions with their high water mark, last stable offset, log start offset, aborted transactions and records.
 * Version 11 (Kafka 2.3, KIP-392) inserted the `preferred_read_replica` between the aborted transactions and the
 * record set of every partition, see {@see \Protocol\Kafka\Protocol\Data\FetchResponsePartition::$preferredReadReplica}.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchResponseV10 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
