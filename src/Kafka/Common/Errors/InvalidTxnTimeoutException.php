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
 * The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms).
 *
 * Error code 50, Kafka 0.11 (InitProducerId, KIP-98).
 */
class InvalidTxnTimeoutException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_TRANSACTION_TIMEOUT, $previous);
    }
}
