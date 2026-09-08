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
 * The connection to the broker could not be established, or the server disconnected before a response was received.
 *
 * This is a client-side condition: Kafka 0.8.2.2 has no wire error code for it (code 13 is StaleLeaderEpoch), so this
 * exception is never produced by KafkaException::fromCode().
 */
class NetworkException extends KafkaException implements RetriableException
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNKNOWN, $previous);
    }
}
