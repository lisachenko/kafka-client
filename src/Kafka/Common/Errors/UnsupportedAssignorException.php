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
 * The assignor or its version range is not supported by the consumer group.
 *
 * Error code 112, Kafka 3.5: KIP-848: a ConsumerGroupHeartbeat (68) named a server-side assignor the coordinator does not have.
 */
class UnsupportedAssignorException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNSUPPORTED_ASSIGNOR, $previous);
    }
}
