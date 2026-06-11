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

/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * OffsetsRequestPartition DTO
 *
 * OffsetsRequestPartition => partition timestamp
 *   partition => INT32
 *   timestamp => INT64
 */
class OffsetsRequestPartition implements BinarySchemaInterface
{
    /**
     * Default constructor
     *
     * @param integer $partition
     * @param integer $timestamp
     */
    public function __construct(
        /**
         * Topic partition id
         */
        public $partition,
        /**
         * The target timestamp for the partition.
         */
        public $timestamp
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
            'timestamp' => BinarySchema::TYPE_INT64,
        ];
    }
}
