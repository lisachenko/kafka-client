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
 * FetchRequestTopic => topic [partitions]
 *   topic => STRING
 *   partitions => partition fetch_offset max_bytes
 *     partition => INT32
 *     fetch_offset => INT64
 *     max_bytes => INT32
 */
class FetchRequestTopic implements BinarySchemaInterface
{
    /**
     * Details about fetching for each topic's partition
     *
     * @var FetchRequestTopicPartition[]
     */
    public $partitions;

    /**
     * @inheritDoc
     * @param string $topic
     */
    public function __construct(/**
     * Name of the topic for fetching
     */
        public $topic,
        array $partitions = []
    ) {
        $this->partitions = $partitions;
    }

    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => FetchRequestTopicPartition::class],
        ];
    }
}
