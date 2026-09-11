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
 * The result of enrolling the partitions of one topic into a transaction, i.e. one entry of the `errors` array
 *
 * <pre>
 *   AddPartitionsToTxnResponseTopic => topic [partition_errors]
 *     topic            => STRING
 *     partition_errors => AddPartitionsToTxnResponsePartition
 * </pre>
 *
 * The array of `ADD_PARTITIONS_TO_TXN_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3 is called `errors` and its inner
 * one `partition_errors`, although both hold the result of *every* requested partition and not only of the ones
 * that failed - the coordinator answers a partition it accepted with the error code 0.
 *
 * @see docs/protocol/2.8.md, section "AddPartitionsToTxn API (key 24, v0 to v3)"
 */
class AddPartitionsToTxnResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this entry belongs to
     */
    public string $topic;

    /**
     * Result of every requested partition of this topic, indexed by the partition id
     *
     * @var array<int, AddPartitionsToTxnResponsePartition>
     */
    public array $partitionErrors;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'           => BinarySchema::TYPE_STRING,
            'partitionErrors' => ['partition' => AddPartitionsToTxnResponsePartition::class],
        ];
    }
}
