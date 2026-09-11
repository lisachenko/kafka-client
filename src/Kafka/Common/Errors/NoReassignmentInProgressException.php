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
 * No partition reassignment is in progress.
 *
 * Error code 85, Kafka 2.4 (KIP-455, AlterPartitionReassignments): an AlterPartitionReassignments (45) asked to cancel
 * (a `null` replica list) the reassignment of a partition that has none in progress.
 */
class NoReassignmentInProgressException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::NO_REASSIGNMENT_IN_PROGRESS, $previous);
    }
}
