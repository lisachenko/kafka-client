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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Heartbeat response
 *
 * Heartbeat Response (Version: 0) => error_code
 *   error_code => INT16
 */
class HeartbeatResponse extends AbstractResponse implements BinarySchemaInterface
{
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
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
