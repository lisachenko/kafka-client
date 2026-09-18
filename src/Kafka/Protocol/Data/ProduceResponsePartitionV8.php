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
 * Partition entry of a Produce answer of the versions 8 and 9
 *
 * The entry of version 8 (Kafka 2.4, KIP-467): the partition index, the error code, the base offset, the log
 * append time, the log start offset, the `record_errors` array and the `error_message`. Version 10 (Kafka 3.7,
 * KIP-951) appended the tagged `current_leader` to it - tag 0, written only for a partition that was refused
 * **6** `NotLeaderForPartition` - which is the only difference between this entry and
 * {@see ProduceResponsePartition}; a version 9 entry is this one in the flexible encoding, with an empty
 * tagged-field section.
 *
 * @see docs/protocol/3.9.md, sections "Produce API (key 0, v0 to v11)" and "The leader discovery of KIP-951 (v10)"
 */
final class ProduceResponsePartitionV8 extends ProduceResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
