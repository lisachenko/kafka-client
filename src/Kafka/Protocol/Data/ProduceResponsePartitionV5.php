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
 * Partition entry of a Produce answer of the versions 5 to 7
 *
 * The entry of version 5 (Kafka 1.0): the partition index, the error code, the base offset, the log append time
 * and the log start offset. Version 8 (Kafka 2.4, KIP-467) appended the `record_errors` array and the
 * `error_message` behind it, see {@see ProduceResponsePartition::$recordErrors}; this class is the entry without
 * them, i.e. what a broker answers a version 5, 6 or 7 request with.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v8)"
 */
final class ProduceResponsePartitionV5 extends ProduceResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
