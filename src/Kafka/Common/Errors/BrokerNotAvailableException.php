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
 * The broker is not available. This error is never returned to a client by a 0.8.2 broker; it is used internally.
 */
class BrokerNotAvailableException extends KafkaException
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::BROKER_NOT_AVAILABLE, $previous);
    }
}
