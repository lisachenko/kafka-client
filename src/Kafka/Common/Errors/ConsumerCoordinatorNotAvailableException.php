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
 * The offsets topic has not been created yet or all of its partitions are still unavailable.
 */
class ConsumerCoordinatorNotAvailableException extends KafkaException implements RetriableException
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::CONSUMER_COORDINATOR_NOT_AVAILABLE, $previous);
    }
}
