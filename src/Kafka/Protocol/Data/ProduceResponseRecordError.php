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
 * One record of a produce request that the broker refused, with the reason it refused it
 *
 * The `BatchIndexAndErrorMessage` of `ProduceResponse.json` @ 2.8.2, added by **KIP-467** (Kafka 2.4) to version
 * **8** of the answer. Until then a batch that failed validation was answered with a single error code for the
 * whole partition - **87** `INVALID_RECORD` most of the time - and a producer had no way of telling *which* of
 * the records of its batch was the bad one; the whole batch had to be dropped or re-sent blindly. Version 8
 * names them:
 *
 * <pre>
 *   RecordErrors => [BatchIndex BatchIndexErrorMessage]
 *     BatchIndex             => int32
 *     BatchIndexErrorMessage => nullable string
 * </pre>
 *
 * {@see self::$batchIndex} is the position of the record **inside the batch that was sent**, counted from 0 -
 * not an offset, and not a position in the log, which the record never reached. The message is the broker's own
 * text (`LogValidator` @ 2.8.2 builds it, e.g. "Compacted topic cannot accept message without key in topic
 * partition t-0"), and it may be `null`: a record that is named without a message of its own is covered by the
 * {@see ProduceResponsePartition::$errorMessage} of the partition.
 *
 * @see docs/protocol/2.8.md, section "The record errors of a refused batch (v8, KIP-467)"
 */
class ProduceResponseRecordError implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO belongs to
     */
    public const int VERSION = 8;

    /**
     * Position of the refused record inside the batch that was sent, counted from 0
     */
    public int $batchIndex = 0;

    /**
     * What the broker refused this record for, `null` when only the partition-wide message says so
     */
    public ?string $batchIndexErrorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'batchIndex'             => BinarySchema::TYPE_INT32,
            'batchIndexErrorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
