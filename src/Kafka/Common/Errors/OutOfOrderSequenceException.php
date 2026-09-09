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
 * The broker received an out of order sequence number.
 *
 * Error code 45, Kafka 0.11 (idempotent producer, KIP-98): the sequence number of a batch is not the one the broker expects next for this producer id and partition; the producer has to fail, a gap can not be filled.
 */
class OutOfOrderSequenceException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::OUT_OF_ORDER_SEQUENCE_NUMBER, $previous);
    }
}
