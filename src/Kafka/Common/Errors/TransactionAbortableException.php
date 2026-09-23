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
 * Error code 120, Kafka 3.8 (KIP-890): the server met an error inside the transaction that the producer can recover from by aborting it, instead of the fatal codes of the older versions. Every api version of the KIP promises to understand it - Produce **v11**, InitProducerId v5, AddPartitionsToTxn v5, AddOffsetsToTxn v4, EndTxn v4, TxnOffsetCommit v4 and FindCoordinator v5 - and a broker answers the code only to a request that carries one of them, see "The abortable transaction error of KIP-890 (v11)" in docs/protocol/3.9.md: a transactional Produce **v11** whose partition the coordinator has not verified is answered 120 where the very same frame at version 10 is answered the 48 `InvalidTxnState`.
 */
class TransactionAbortableException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::TRANSACTION_ABORTABLE, $previous);
    }
}
