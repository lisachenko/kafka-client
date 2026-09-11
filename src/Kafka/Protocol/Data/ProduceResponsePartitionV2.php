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
 * Produce response partition DTO of the versions 2, 3 and 4
 *
 * <pre>
 *   Partition ErrorCode Offset LogAppendTime
 *     Partition     => int32
 *     ErrorCode     => int16
 *     Offset        => int64
 *     LogAppendTime => int64
 * </pre>
 *
 * `PRODUCE_RESPONSE_V4` is `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2` in
 * `ProduceResponse.schemaVersions()` @ 1.1.1, so these three versions share one partition entry: the one with the
 * `LogAppendTime` of version 2 and without the `LogStartOffset` of version 5. This class only lowers the version
 * constant that {@see ProduceResponsePartition::getScheme()} follows; `$logStartOffset` keeps its default of -1,
 * "the answer did not say".
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v6)"
 */
final class ProduceResponsePartitionV2 extends ProduceResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
