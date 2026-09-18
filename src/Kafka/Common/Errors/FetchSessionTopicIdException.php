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
 * The fetch session encountered inconsistent topic ID usage.
 *
 * Error code 106, Kafka 3.1: A Fetch v13 (KIP-516, topics named by id) used a fetch session (KIP-227) whose earlier requests named the topics by name, or the other way round; the session has to be started anew. A `RetriableException` in the Java client, so retriable here.
 */
class FetchSessionTopicIdException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::FETCH_SESSION_TOPIC_ID_ERROR, $previous);
    }
}
