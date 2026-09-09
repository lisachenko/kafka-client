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
 * The producer attempted to use a producer id which is not currently assigned to its transactional id.
 *
 * Error code 49, Kafka 0.11 (transactions, KIP-98).
 */
class InvalidPidMappingException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_PRODUCER_ID_MAPPING, $previous);
    }
}
