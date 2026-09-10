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
 * The result of deleting the records of one topic, i.e. one entry of the `topics` array
 *
 * <pre>
 *   DeleteRecordsResponseTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => DeleteRecordsResponsePartition
 * </pre>
 *
 * `DELETE_RECORDS_RESPONSE_TOPIC_V0` in `Protocol.java` @ 0.11.0.3. The answer holds one entry per partition of the
 * REQUEST - a topic the broker does not know at all is reported here as well, with the error code 3 for each of its
 * partitions, and never left out.
 *
 * @see docs/protocol/1.1.md, section "DeleteRecords API (key 21, v0)"
 */
class DeleteRecordsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this entry belongs to
     */
    public string $topic;

    /**
     * Result of every requested partition of this topic, indexed by the partition id
     *
     * @var array<int, DeleteRecordsResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => DeleteRecordsResponsePartition::class],
        ];
    }
}
