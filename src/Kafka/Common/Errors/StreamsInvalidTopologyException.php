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
 * The supplied topology is invalid.
 *
 * Error code 130, Kafka 4.1: KIP-1071 streams groups: the topology of a StreamsGroupHeartbeat (88) is not valid.
 */
class StreamsInvalidTopologyException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::STREAMS_INVALID_TOPOLOGY, $previous);
    }
}
