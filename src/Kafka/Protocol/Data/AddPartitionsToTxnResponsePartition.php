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
 * The result of enrolling one partition into a transaction, i.e. one entry of a `partition_errors` array
 *
 * <pre>
 *   AddPartitionsToTxnResponsePartition => partition error_code
 *     partition  => INT32
 *     error_code => INT16
 * </pre>
 *
 * Error codes a 0.11.0.3 coordinator reports here, from `TransactionCoordinator.handleAddPartitionsToTransaction`
 * and `KafkaApis.handleAddPartitionToTxnRequest`:
 *
 * | Code | Name                               | Meaning                                                          |
 * |------|------------------------------------|------------------------------------------------------------------|
 * | 0    | None                               | The partition is part of the transaction, or was already          |
 * | 15   | GroupCoordinatorNotAvailable       | The coordinator of the id is not available on this broker         |
 * | 16   | NotCoordinatorForGroup             | Another broker coordinates this transactional id                  |
 * | 47   | InvalidProducerEpoch               | The epoch is below the one `__transaction_state` holds - fenced   |
 * | 48   | InvalidTxnState                    | The id is in a state that may not add partitions (a transaction that is being completed) |
 * | 49   | InvalidProducerIdMapping           | The producer id is not the one the coordinator holds for the id   |
 * | 51   | ConcurrentTransactions             | The previous transaction of the id is still being completed       |
 * | 29   | TopicAuthorizationFailed           | The client may not `Write` this topic                             |
 * | 55   | OperationNotAttempted              | Another partition of the same request failed, so this one was not even tried |
 *
 * **55 is the code of this api and of no other**: `KafkaApis` answers a request in which any partition is
 * unauthorized with `OperationNotAttempted` for every partition it did not look at, so that a client cannot
 * conclude from a missing error that a partition was added.
 *
 * @see docs/protocol/2.8.md, section "AddPartitionsToTxn API (key 24, v0)"
 */
class AddPartitionsToTxnResponsePartition implements BinarySchemaInterface
{
    /**
     * Id of the partition this entry belongs to
     */
    public int $partition;

    /**
     * Error code of that partition, 0 when it is part of the transaction
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
