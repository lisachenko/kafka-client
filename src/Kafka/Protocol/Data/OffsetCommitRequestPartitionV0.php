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
 * OffsetCommitRequestPartition DTO, version 0 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestPartition => partition offset metadata
 *     partition => INT32
 *     offset    => INT64
 *     metadata  => NULLABLE_STRING
 * </pre>
 *
 * The `timestamp` field only exists in version 1, so this class exists solely to lower the version constant that
 * drives {@see OffsetCommitRequestPartition::getScheme()}; the bytes it packs are the ones of version 2.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v6)"
 */
final class OffsetCommitRequestPartitionV0 extends OffsetCommitRequestPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
