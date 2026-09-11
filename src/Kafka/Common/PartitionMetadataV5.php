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

namespace Protocol\Kafka\Common;

/**
 * Partition metadata of a Metadata answer of the versions 5 and 6
 *
 * <pre>
 *   partition_error_code partition_id leader [replicas] [isr] [offline_replicas]
 * </pre>
 *
 * The entry of version 5 (Kafka 1.0, KIP-112/113) is the one version 6 (Kafka 2.0, KIP-219) answers as well, and
 * it is the last one without a `leader_epoch`: version 7 (Kafka 2.1, KIP-320) inserted that field behind the
 * leader id, see {@see PartitionMetadata::$leaderEpoch}. A client that asks with version 6 or lower therefore has
 * no way of telling one leadership of a partition from the next.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v8)"
 */
final class PartitionMetadataV5 extends PartitionMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
