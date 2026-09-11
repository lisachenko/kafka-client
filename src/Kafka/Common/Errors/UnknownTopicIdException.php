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
 * This server does not host this topic ID.
 *
 * Error code 100, Kafka 2.8 (KIP-516, topic ids): a request that names a topic by its id (DeleteTopics v6, and on a
 * 2.8 broker the controller apis) named one the broker does not host. An `InvalidMetadataException` in the Java
 * client, so retriable.
 */
class UnknownTopicIdException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNKNOWN_TOPIC_ID, $previous);
    }
}
