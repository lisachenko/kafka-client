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
 * Partition entry of a Metadata response of the versions 0 to 4, i.e. the one without `OfflineReplicas`
 *
 * <pre>
 *   PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
 * </pre>
 *
 * `PARTITION_METADATA_V1` is `PARTITION_METADATA_V0` in `MetadataResponse.java` @ 1.1.1: the partition entry did
 * not change between the versions 0 and 4, and the `OfflineReplicas` of version 5 (Kafka 1.0, KIP-112/113) is the
 * first field it ever gained. This class only lowers the version constant that
 * {@see PartitionMetadata::getScheme()} follows; `$offlineReplicas` keeps its empty default, which is "the answer
 * did not say".
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v8)"
 */
final class PartitionMetadataV0 extends PartitionMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
