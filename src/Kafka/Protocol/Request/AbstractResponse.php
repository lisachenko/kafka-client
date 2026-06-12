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

use Protocol\Kafka\Protocol\AbstractProtocolMessage;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Basic class for all responses
 */
abstract class AbstractResponse extends AbstractProtocolMessage
{
    /**
     * A user-supplied integer value that will be passed back with the response (INT32)
     *
     * @var integer
     */
    public $correlationId;

    public static function getScheme(): array
    {
        return [
            'messageSize'   => BinarySchema::TYPE_INT32,
            'correlationId' => BinarySchema::TYPE_INT32,
        ];
    }
}
