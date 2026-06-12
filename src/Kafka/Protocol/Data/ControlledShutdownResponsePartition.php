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
 * ControlledShutdownResponsePartition DTO
 *
 * ControlledShutdownResponsePartition => partition timestamp
 *   topic => STRING
 *   partition => INT32
 */
class ControlledShutdownResponsePartition implements BinarySchemaInterface
{
    /**
     * Name of topic
     *
     * @var string
     */
    public $topic;

    /**
     * Topic partition id
     *
     * @var integer
     */
    public $partition;

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
