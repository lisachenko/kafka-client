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
 * Topic deletion is disabled.
 *
 * Error code 73, Kafka 2.1 (DeleteTopics): the broker runs with `delete.topic.enable=false`; a 2.0 broker answered
 * such a DeleteTopics with 41 or silently ignored the request, a 2.1 broker names the reason.
 */
class TopicDeletionDisabledException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::TOPIC_DELETION_DISABLED, $previous);
    }
}
