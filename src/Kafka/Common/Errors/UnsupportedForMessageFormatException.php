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
 * The message format version on the broker does not support the request.
 *
 * Error code 43, Kafka 0.10.2 (Offsets v1 by timestamp on a topic with message format v0, KIP-79).
 */
class UnsupportedForMessageFormatException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNSUPPORTED_FOR_MESSAGE_FORMAT, $previous);
    }
}
