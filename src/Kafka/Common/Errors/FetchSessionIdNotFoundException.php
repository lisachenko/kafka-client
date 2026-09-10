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
 * The fetch session ID was not found.
 *
 * Error code 70, Kafka 1.1 (Fetch v7, incremental fetch sessions of KIP-227): the broker evicted the session the request continues, or never had it. It is retriable in the Java client: the consumer answers it with a new full fetch (`session_id = 0`, `epoch = 0`), which creates a new session.
 */
class FetchSessionIdNotFoundException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::FETCH_SESSION_ID_NOT_FOUND, $previous);
    }
}
