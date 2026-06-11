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
 * Offsets response DTO
 *
 * OffsetsResponsePartition => partition error_code timestamp offset
 *   partition => INT32
 *   error_code => INT16
 *   timestamp => INT64
 *   offset => INT64
 */
class OffsetsResponsePartition implements BinarySchemaInterface
{
    /**
     * The partition this response entry corresponds to.
     *
     * @var integer
     */
    public $partition;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have successfully accepted the produce request.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * The timestamp associated with the returned offset
     *
     * @since 0.10.1
     *
     * @var integer
     */
    public $timestamp;

    /**
     * Found offset
     *
     * @var integer
     */
    public $offset;

    /**
     * Returns definition of binary packet for the class or object
     *
     * @return array
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'errorCode' => BinarySchema::TYPE_INT16,
            'timestamp' => BinarySchema::TYPE_INT64,
            'offset'    => BinarySchema::TYPE_INT64,
        ];
    }
}
