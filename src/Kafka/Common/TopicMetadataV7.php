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
 * Topic entry of a Metadata answer of version 7
 *
 * The entry of version 7 (Kafka 2.1, KIP-320): the error code, the name, the internal flag and the partitions,
 * whose entries carry the `leader_epoch` of that release. Version 8 (Kafka 2.3, KIP-430) appended the
 * `topic_authorized_operations` bitfield behind the partitions, see {@see TopicMetadata::$authorizedOperations};
 * this class is the entry without it.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
final class TopicMetadataV7 extends TopicMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
