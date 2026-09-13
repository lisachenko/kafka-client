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
 * There is a newer producer with the same transactionalId which fences the current one.
 *
 * Error code 90, Kafka 2.7 (KIP-588): the transaction coordinator answers the requests of a producer whose epoch has
 * been superseded by a newer producer of the same transactional id with this code, where 2.6 and below answered 47
 * `INVALID_PRODUCER_EPOCH` for the same situation; the versions that promise to understand it (InitProducerId v4,
 * AddPartitionsToTxn v2, AddOffsetsToTxn v2, EndTxn v2, TxnOffsetCommit v3) are answered with 90, older ones keep
 * getting 47. Fatal for the fenced producer, exactly like 47. The Java client calls this class
 * `ProducerFencedException` (and renamed the class of 47 to `InvalidProducerEpochException` with 2.7); this package
 * keeps `ProducerFencedException` as the name of the code 47 it has published since the 0.11 line, so the new code
 * carries the transactional qualifier.
 */
class TransactionalProducerFencedException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::PRODUCER_FENCED, $previous);
    }
}
