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
 * OffsetsRequestPartition DTO
 *
 * OffsetsRequestPartition => partition timestamp
 *   partition => INT32
 *   timestamp => INT64
 */
class OffsetsRequestPartition implements BinarySchemaInterface
{
    /**
     * Topic partition id
     * @var int
     */
    public $partition;

    /**
     * The target timestamp for the partition.
     * @var int
     */
    public $timestamp;

    /**
     * OffsetsRequestPartition constructor.
     *
     * @param int $partition Topic partition id
     * @param int $timestamp The target timestamp for the partition.
     */
    public function __construct(int $partition, int $timestamp)
    {
        $this->partition = $partition;
        $this->timestamp = $timestamp;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'timestamp' => BinarySchema::TYPE_INT64,
        ];
    }
}
