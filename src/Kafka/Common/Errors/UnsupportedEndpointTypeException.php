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
 * This endpoint type is not supported yet.
 *
 * Error code 115, Kafka 3.7: KIP-919: a DescribeCluster v1 asked for an endpoint type the node does not know.
 */
class UnsupportedEndpointTypeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNSUPPORTED_ENDPOINT_TYPE, $previous);
    }
}
