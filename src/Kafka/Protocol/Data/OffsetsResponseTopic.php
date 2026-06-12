<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * OffsetsResponseTopic DTO
 *
 * OffsetsResponseTopic => topic [partition_responses]
 *   topic => STRING
 *   partition_responses => partition error_code timestamp offset
 *     partition => INT32
 *     error_code => INT16
 *     timestamp => INT64
 *     offset => INT64
 */
class OffsetsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     *
     * @var string
     */
    public $topic;

    /**
     * Partition responses.
     *
     * @var OffsetsResponsePartition[]
     */
    public $partitions;

    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => OffsetsResponsePartition::class],
        ];
    }
}
