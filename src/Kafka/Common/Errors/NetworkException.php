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

namespace Protocol\Kafka\Common\Errors;

use Exception;

/**
 * The server disconnected before a response was received.
 */
class NetworkException extends KafkaException implements RetriableException
{
    public function __construct(array $context, ?Exception $previous = null)
    {
        parent::__construct($context, self::NETWORK_EXCEPTION, $previous);
    }
}
