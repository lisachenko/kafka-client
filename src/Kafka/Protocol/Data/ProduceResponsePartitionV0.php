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
 * Produce response partition DTO of the versions 0 and 1
 *
 * <pre>
 *   Partition ErrorCode Offset
 *     Partition => int32
 *     ErrorCode => int16
 *     Offset    => int64
 * </pre>
 *
 * The `LogAppendTime` of version 2 does not exist in the answer of a version 0 or 1 request, so this class only
 * lowers the version constant that {@see ProduceResponsePartition::getScheme()} follows; `$logAppendTime` keeps
 * its default of -1, "the broker reported none".
 *
 * @see docs/protocol/0.11.0.md, section "Produce API (key 0, v0 to v3)"
 */
final class ProduceResponsePartitionV0 extends ProduceResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
