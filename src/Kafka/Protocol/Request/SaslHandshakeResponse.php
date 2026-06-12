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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * SASL handshake response
 */
class SaslHandshakeResponse extends AbstractResponse implements BinarySchemaInterface
{
    /**
     * Array of mechanisms enabled in the server.
     *
     * @var array|string[]
     */
    public $enabledMechanisms = [];

    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'         => BinarySchema::TYPE_INT16,
            'enabledMechanisms' => [BinarySchema::TYPE_STRING],
        ];
    }
}
