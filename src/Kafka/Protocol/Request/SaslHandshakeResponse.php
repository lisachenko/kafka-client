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

/**
 * SASL handshake response
 */
class SaslHandshakeResponse extends AbstractResponse
{
    /**
     * Array of mechanisms enabled in the server.
     *
     * @var string[]
     */
    public $enabledMechanisms = [];

    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'         => BinarySchema::TYPE_INT16,
            'enabledMechanisms' => [BinarySchema::TYPE_STRING],
        ];
    }
}
