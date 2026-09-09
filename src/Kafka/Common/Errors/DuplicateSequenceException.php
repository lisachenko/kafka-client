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
 * The broker received a duplicate sequence number.
 *
 * Error code 46, Kafka 0.11 (idempotent producer, KIP-98): the batch was already appended, a retry after a lost acknowledgement; the Java client treats it as a success.
 */
class DuplicateSequenceException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DUPLICATE_SEQUENCE_NUMBER, $previous);
    }
}
