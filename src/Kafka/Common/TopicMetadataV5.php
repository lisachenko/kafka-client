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
 * Topic metadata of a Metadata answer of the versions 5 and 6
 *
 * The topic entry itself did not change between version 1 and version 8; what the version constant selects is the
 * shape of its partition entries, see {@see TopicMetadata::partitionClass()}. The versions 5 and 6 answer with
 * {@see PartitionMetadataV5}, which carries the `offline_replicas` of KIP-112/113 and no leader epoch.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v7)"
 */
final class TopicMetadataV5 extends TopicMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
