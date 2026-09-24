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
 * One topic of the current transaction of a DescribeTransactions answer (key 65, Kafka 3.0)
 *
 * <pre>
 *   TopicData => Topic Partitions
 *     Topic      => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of INT32
 * </pre>
 *
 * `TopicData` of `DescribeTransactionsResponse.json` @ 3.0.2, whose first field is called `Topic` and not `Name`
 * as in every other api of this protocol. The partitions are the ones an `AddPartitionsToTxn` of this producer
 * added to the transaction that is open now - the partitions its commit or abort marker still has to reach.
 *
 * @see docs/protocol/4.3.md, section "DescribeTransactions API (key 65, v0)"
 */
class DescribeTransactionsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic a partition of the current transaction belongs to
     */
    public string $topic;

    /**
     * Partitions of this topic that are part of the current transaction
     *
     * @var list<int>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}
