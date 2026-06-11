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
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * OffsetCommitResponsePartition DTO
 *
 * OffsetCommitResponsePartition => partition error_code
 *   partition => INT32
 *   error_code => INT16
 */
class OffsetCommitResponsePartition implements BinarySchemaInterface
{
    /**
     * The partition this request entry corresponds to.
     *
     * @var integer
     */
    public $partition;

    /**
     * The error from this partition, if any.
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
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
