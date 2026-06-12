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
 * OffsetCommitRequestPartition DTO
 *
 * OffsetCommitRequestPartition => partition offset metadata
 *   partition => INT32
 *   offset => INT64
 *   metadata => NULLABLE_STRING
 */
class OffsetCommitRequestPartition implements BinarySchemaInterface
{
    /**
     * @param int $partition
     * @param int $offset
     * @param string $metadata
     */
    public function __construct(
        /**
         * The partition this request entry corresponds to.
         */
        public $partition,
        /**
         * The offset assigned to the first message in the message set appended to this partition.
         */
        public $offset,
        /**
         * Any associated metadata the client wants to keep.
         */
        public $metadata = null
    ) {}

    /**
     * Returns definition of binary packet for the class or object
     *
     * @return array
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'offset'    => BinarySchema::TYPE_INT64,
            'metadata'  => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
