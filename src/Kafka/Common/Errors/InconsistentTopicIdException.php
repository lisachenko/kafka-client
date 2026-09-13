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
 * The log's topic ID did not match the topic ID in the request
 *
 * Error code 103, Kafka 2.8 (KIP-516, topic ids): the topic id of a request does not match the id the broker stores in
 * the partition metadata file of the log. An `InvalidMetadataException` in the Java client, so retriable.
 */
class InconsistentTopicIdException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INCONSISTENT_TOPIC_ID, $previous);
    }
}
