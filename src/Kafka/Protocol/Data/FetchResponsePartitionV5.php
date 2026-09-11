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

namespace Protocol\Kafka\Protocol\Data;

/**
 * Partition entry of a Fetch answer of the versions 5 to 10
 *
 * The entry the versions 5 to 10 answer: the high water mark, the last stable offset, the log start offset of
 * KIP-107, the nullable aborted-transactions array and the record set. Version 11 (Kafka 2.3, KIP-392) inserted
 * the `preferred_read_replica` between the aborted transactions and the records, see
 * {@see FetchResponsePartition::$preferredReadReplica}; this class is the entry without it, and
 * {@see FetchResponsePartition::NO_PREFERRED_READ_REPLICA} is what its property keeps.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchResponsePartitionV5 extends FetchResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
