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
 * Eligible topic partition leaders are not available.
 *
 * Error code 83, Kafka 2.4 (KIP-460, ElectLeaders v1): an unclean election was requested for a partition that has no
 * live replica to elect. An `InvalidMetadataException` in the Java client, so retriable.
 */
class EligibleLeadersNotAvailableException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::ELIGIBLE_LEADERS_NOT_AVAILABLE, $previous);
    }
}
