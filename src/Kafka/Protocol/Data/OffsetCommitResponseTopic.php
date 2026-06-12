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
 * OffsetCommitResponseTopic DTO
 *
 * OffsetCommitResponseTopic => topic [partition_responses]
 *   topic => STRING
 *   partition_responses => partition error_code
 *     partition => INT32
 *     error_code => INT16
 */
class OffsetCommitResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     *
     * @var string
     */
    public $topic;

    /**
     * Result for offset committing by each topic-partition
     *
     * @var OffsetCommitResponsePartition[]
     */
    public $partitions;

    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => OffsetCommitResponsePartition::class],
        ];
    }
}
