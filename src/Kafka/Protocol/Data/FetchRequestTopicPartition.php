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
 * Class FetchRequestTopicPartition
 *
 * FetchRequestTopicPartition => partition fetch_offset log_start_offset max_bytes
 *   partition => INT32
 *   fetch_offset => INT64
 *   log_start_offset => INT64 (Since FetchRequest v5)
 *   max_bytes => INT32
 */
class FetchRequestTopicPartition implements BinarySchemaInterface
{
    /**
     * @param int $partition
     * @param int $fetchOffset
     * @param int $logStartOffset
     * @param int $maxBytes
     */
    public function __construct(
        /**
         * Topic partition id
         */
        public $partition,
        /**
         * Record offset.
         */
        public $fetchOffset,
        /**
         * Maximum bytes to fetch.
         */
        public $maxBytes,
        /**
         * Earliest available offset of the follower replica.
         *
         * The field is only used when request is sent by follower.
         *
         * @since 0.11.0.0 Kafka
         */
        public $logStartOffset = -1
    ) {}

    public static function getScheme(): array
    {
        return [
            'partition'      => BinarySchema::TYPE_INT32,
            'fetchOffset'    => BinarySchema::TYPE_INT64,
            'logStartOffset' => BinarySchema::TYPE_INT64,
            'maxBytes'       => BinarySchema::TYPE_INT32,
        ];
    }
}
