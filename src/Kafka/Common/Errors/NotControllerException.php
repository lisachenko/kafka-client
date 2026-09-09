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
 * This is not the correct controller for this cluster.
 *
 * Error code 41, Kafka 0.10.1 (CreateTopics/DeleteTopics, KIP-4): the request was sent to a broker that is not the controller; retriable after a metadata refresh, which carries the controller id from Metadata v1 on.
 */
class NotControllerException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::NOT_CONTROLLER, $previous);
    }
}
