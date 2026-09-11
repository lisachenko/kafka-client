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
 * OffsetCommitRequestPartition DTO, version 1 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestPartition => partition offset timestamp metadata
 *     partition => INT32
 *     offset    => INT64
 *     timestamp => INT64  (version 1 only)
 *     metadata  => NULLABLE_STRING
 * </pre>
 *
 * Version 1 is the only version of the request with a per-partition commit timestamp, so this class exists solely
 * to raise that field into the scheme that {@see OffsetCommitRequestPartition::getScheme()} builds.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v7)"
 */
final class OffsetCommitRequestPartitionV1 extends OffsetCommitRequestPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
