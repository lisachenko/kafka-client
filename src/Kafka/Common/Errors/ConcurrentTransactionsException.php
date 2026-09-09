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
 * The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
 *
 * Error code 51, Kafka 0.11 (transactions, KIP-98): the coordinator is still completing the previous transaction of this transactional id; the Java client retries the request after a short back-off.
 */
class ConcurrentTransactionsException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::CONCURRENT_TRANSACTIONS, $previous);
    }
}
