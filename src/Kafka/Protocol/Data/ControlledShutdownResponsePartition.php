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
 * One topic-partition that the controller could not move off the broker that is shutting down.
 *
 * <pre>
 *   TopicAndPartition => TopicName Partition
 *     TopicName => string
 *     Partition => int32
 * </pre>
 *
 * Unlike every other response of the protocol, the entries are a flat list of topic-partition pairs instead of
 * partitions grouped by their topic, so the same topic may appear more than once.
 *
 * @see docs/protocol/2.8.md, section "ControlledShutdown API (key 7, v0 to v3)"
 */
class ControlledShutdownResponsePartition implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $topic = '';

    /**
     * Number of the partition that still has a leader or a replica on the broker
     */
    public int $partition = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'     => BinarySchema::TYPE_STRING,
            'partition' => BinarySchema::TYPE_INT32,
        ];
    }
}
