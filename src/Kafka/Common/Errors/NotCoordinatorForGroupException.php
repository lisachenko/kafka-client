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
 * This is not the correct coordinator for this group.
 *
 * Named NotCoordinatorForConsumerCode (16) in kafka/common/ErrorMapping.scala @ 0.8.2.2.
 */
class NotCoordinatorForGroupException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::NOT_COORDINATOR_FOR_GROUP, $previous);
    }
}
