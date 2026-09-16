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
 * The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.
 *
 * Error code 120, Kafka 3.8: The transaction protocol v2 of KIP-890 (InitProducerId v5, AddPartitionsToTxn v5, AddOffsetsToTxn v4, EndTxn v4, TxnOffsetCommit v4, FindCoordinator v5): the server met an error inside the transaction that the producer can recover from by aborting it, instead of the fatal codes of the older versions.
 */
class TransactionAbortableException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::TRANSACTION_ABORTABLE, $previous);
    }
}
