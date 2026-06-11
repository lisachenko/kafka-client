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
 * @date   14.07.2016
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Join group request protocol DTO
 *
 * JoinGroupRequestProtocol => protocol_name protocol_metadata
 *   protocol_name => STRING
 *   protocol_metadata => BYTES
 */
class JoinGroupRequestProtocol implements BinarySchemaInterface
{
    /**
     * Default initializer
     * @param string $name
     * @param string $metadata
     */
    public function __construct(
        /**
         * Name of the protocol
         */
        public $name,
        /**
         * Protocol-specific metadata
         */
        public $metadata
    ) {}

    public static function getScheme(): array
    {
        return [
            'name'     => BinarySchema::TYPE_STRING,
            'metadata' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
