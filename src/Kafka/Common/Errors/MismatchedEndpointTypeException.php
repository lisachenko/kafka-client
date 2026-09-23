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
 * The request was sent to an endpoint of the wrong type.
 *
 * Error code 114, Kafka 3.7: KIP-919: a DescribeCluster v1 asked a broker for the controller endpoints, or a controller for the broker endpoints.
 */
class MismatchedEndpointTypeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::MISMATCHED_ENDPOINT_TYPE, $previous);
    }
}
