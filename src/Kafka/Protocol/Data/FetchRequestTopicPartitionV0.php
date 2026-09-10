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
 * One partition of a Fetch request of the versions 0 to 4
 *
 * <pre>
 *   FetchRequestTopicPartition => Partition FetchOffset MaxBytes
 * </pre>
 *
 * The `LogStartOffset` that version 5 added between the fetch offset and the per-partition `MaxBytes` does not
 * exist below it, so this class only lowers the version constant that
 * {@see FetchRequestTopicPartition::getScheme()} follows.
 *
 * @see docs/protocol/1.1.md, section "Fetch API (key 1, v0 to v5)"
 */
final class FetchRequestTopicPartitionV0 extends FetchRequestTopicPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
