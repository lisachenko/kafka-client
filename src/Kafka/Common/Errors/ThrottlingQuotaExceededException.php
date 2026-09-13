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
 * The throttling quota has been exceeded.
 *
 * Error code 89, Kafka 2.7 (KIP-599): a CreateTopics v6, CreatePartitions v3 or DeleteTopics v5 exceeded the
 * `controller_mutation_rate` quota of the principal; the broker refuses the mutation instead of delaying it and the
 * answer carries the throttle time to wait. Retriable in the Java client.
 */
class ThrottlingQuotaExceededException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::THROTTLING_QUOTA_EXCEEDED, $previous);
    }
}
