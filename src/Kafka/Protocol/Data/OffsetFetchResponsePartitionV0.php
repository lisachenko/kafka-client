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
 * OffsetFetchResponsePartition DTO, the versions 0 to 4 of the OffsetFetch API
 *
 * <pre>
 *   OffsetFetchResponsePartition => partition offset metadata error_code
 *     partition  => INT32
 *     offset     => INT64
 *     metadata   => NULLABLE_STRING
 *     error_code => INT16
 * </pre>
 *
 * This is the entry of every version below 5; version 5 (Kafka 2.1, KIP-320) inserted the
 * `committed_leader_epoch` between the offset and the metadata, see {@see OffsetFetchResponsePartition}.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v6)"
 */
final class OffsetFetchResponsePartitionV0 extends OffsetFetchResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
