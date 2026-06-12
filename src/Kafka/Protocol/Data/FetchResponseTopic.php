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
 * Fetch request topic DTO
 *
 * FetchResponseTopic => topic [partition_responses]
 *   topic => STRING
 *   partition_responses => partition_header record_set
 *     partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions]
 *       partition => INT32
 *       error_code => INT16
 *       high_watermark => INT64
 *       last_stable_offset => INT64
 *       log_start_offset => INT64
 *       aborted_transactions => producer_id first_offset
 *         producer_id => INT64
 *         first_offset => INT64
 *   record_set => RECORDS
 */
class FetchResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic for fetching
     *
     * @var string
     */
    public $topic;

    /**
     * Details about fetching for each topic's partition
     *
     * @var FetchResponsePartition[]
     */
    public $partitions;

    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => FetchResponsePartition::class],
        ];
    }
}
