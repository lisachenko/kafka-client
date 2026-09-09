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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResponseTopic;

/**
 * AddPartitionsToTxn response object, version 0 (key 24)
 *
 * <pre>
 *   AddPartitionsToTxn Response (Version: 0) => throttle_time_ms [errors]
 *     throttle_time_ms => INT32
 *     errors           => topic [partition_errors]
 *       topic            => STRING
 *       partition_errors => partition error_code
 *         partition  => INT32
 *         error_code => INT16
 * </pre>
 *
 * The api was born in Kafka 0.11, after KIP-124 made `throttle_time_ms` the first field of every new answer, so
 * there is no version of it without one.
 *
 * There is **no top-level error code**: everything the coordinator has to say - including the errors that are
 * really about the transactional id rather than about a partition, 47, 48, 49 and 51 - is repeated on every
 * partition of the answer, because `KafkaApis.handleAddPartitionToTxnRequest` @ 0.11.0.3 builds the answer by
 * mapping the single error of `handleAddPartitionsToTransaction` over the requested partitions. A client therefore
 * has to look at the partitions to learn what happened to the transaction, which is what
 * {@see \Protocol\Kafka\Client::addPartitionsToTxn()} does.
 *
 * @see docs/protocol/0.11.0.md, section "AddPartitionsToTxn API (key 24, v0)"
 */
class AddPartitionsToTxnResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every requested topic, indexed by the topic name
     *
     * @var array<string, AddPartitionsToTxnResponseTopic>
     */
    public array $errors = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errors'         => ['topic' => AddPartitionsToTxnResponseTopic::class],
        ];
    }
}
