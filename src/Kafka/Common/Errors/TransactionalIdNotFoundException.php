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
 * The transactionalId could not be found.
 *
 * Error code 105, Kafka 3.0: DescribeTransactions (65) names a transactional id the coordinator has no state for.
 */
class TransactionalIdNotFoundException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::TRANSACTIONAL_ID_NOT_FOUND, $previous);
    }
}
