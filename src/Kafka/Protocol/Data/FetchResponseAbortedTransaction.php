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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One aborted transaction of a Fetch response partition
 *
 * <pre>
 *   FetchResponseAbortedTransaction => ProducerId FirstOffset
 *     ProducerId  => int64
 *     FirstOffset => int64
 * </pre>
 *
 * A `read_committed` fetch (`isolation_level = 1`) is answered with the transactions that were **aborted** in the
 * range the answer covers: the id of the producer that wrote them and the first offset it wrote in that
 * transaction. The broker reads them out of the `.txnindex` file of the segment
 * (`ProducerStateManager`/`TransactionIndex` @ 0.11.0.3), it does not filter the records itself - the client drops
 * the records of an aborted producer until it sees the abort marker of that producer, which is why the answer also
 * carries the control batches.
 *
 * A `read_uncommitted` fetch never gets the list at all: the whole array is `null` there, not empty, see
 * {@see FetchResponsePartition::$abortedTransactions}.
 *
 * @since Version 4 of the Fetch API (Kafka 0.11.0, KIP-98)
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
class FetchResponseAbortedTransaction implements BinarySchemaInterface
{
    /**
     * Id of the producer whose transaction was aborted
     */
    public int $producerId = 0;

    /**
     * First offset that this producer wrote in the aborted transaction
     */
    public int $firstOffset = 0;

    public function __construct(int $producerId = 0, int $firstOffset = 0)
    {
        $this->producerId  = $producerId;
        $this->firstOffset = $firstOffset;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'producerId'  => BinarySchema::TYPE_INT64,
            'firstOffset' => BinarySchema::TYPE_INT64,
        ];
    }
}
