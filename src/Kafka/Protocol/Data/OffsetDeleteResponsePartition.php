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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of one partition of an OffsetDelete answer (key 47, Kafka 2.4, KIP-496)
 *
 * <pre>
 *   OffsetDeleteResponsePartition => PartitionIndex ErrorCode
 *     PartitionIndex => INT32
 *     ErrorCode      => INT16
 * </pre>
 *
 * There is no `error_message`: the code is everything the coordinator says about a partition. What was measured on
 * the container is **0** for a partition whose committed offset is gone - and also for one that never had a
 * committed offset, because `GroupCoordinator.handleDeleteOffsets` @ 2.8.2 answers `NONE` for every partition it
 * considers eligible without looking whether it removed anything - **3** `UnknownTopicOrPartition` for a topic or
 * a partition this broker's metadata cache does not have (`KafkaApis.handleOffsetDeleteRequest` sorts those out
 * before the coordinator sees them), **29** `TopicAuthorizationFailed` for a topic the client may not read, and
 * **86** `GroupSubscribedToTopic` for a topic a live member of the group is subscribed to.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
class OffsetDeleteResponsePartition implements BinarySchemaInterface
{
    /**
     * Index of the partition this result belongs to
     */
    public int $partitionIndex;

    /**
     * Error code of this partition, 0 when the committed offset of it is gone
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
        ];
    }
}
