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
 * OffsetFetchResponsePartition DTO
 *
 * OffsetFetchResponsePartition => partition offset metadata error_code
 *   partition => INT32
 *   offset => INT64
 *   metadata => NULLABLE_STRING
 *   error_code => INT16
 */
class OffsetFetchResponsePartition implements BinarySchemaInterface
{
    /**
     * The partition this response entry corresponds to.
     *
     * @var integer
     */
    public $partition;

    /**
     * The offset assigned to the first message in the message set appended to this partition.
     *
     * @var integer
     */
    public $offset;

    /**
     * Any associated metadata the client wants to keep.
     *
     * @var string
     */
    public $metadata;

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
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
