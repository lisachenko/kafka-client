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
 * The fetch session epoch is invalid.
 *
 * Error code 71, Kafka 1.1 (Fetch v7, incremental fetch sessions of KIP-227): the epoch of the request is not the one the broker expects next for the session, for instance after a lost answer. It is retriable in the Java client: the consumer closes the session with a new full fetch.
 */
class InvalidFetchSessionEpochException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_FETCH_SESSION_EPOCH, $previous);
    }
}
