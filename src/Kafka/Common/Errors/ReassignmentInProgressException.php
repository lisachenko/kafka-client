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
 * A partition reassignment is in progress.
 *
 * Error code 60, Kafka 1.0 (CreatePartitions, KIP-195): the partitions of a topic can not be added while a reassignment of its replicas is running.
 */
class ReassignmentInProgressException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::REASSIGNMENT_IN_PROGRESS, $previous);
    }
}
