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
 * The result of one transaction of an AddPartitionsToTxn answer of version 4 (Kafka 3.5, KIP-890)
 *
 * <pre>
 *   AddPartitionsToTxnResult => transactional_id [topic_results]
 *     transactional_id => COMPACT_STRING
 *     topic_results    => AddPartitionsToTxnResponseTopic
 * </pre>
 *
 * **Version 4 answers one entry per transaction of the request** (`AddPartitionsToTxnResponse.json` @ 3.5.2:
 * *"Version 4 adds support to batch multiple transactions and a top level error code"*), each of them the topic
 * array the versions 0 to 3 carried at the top level, behind the transactional id it belongs to. The entries come
 * in the order in which the coordinator finished the transactions of the batch, which is not necessarily the order
 * of the request, so a reader looks an entry up by its id
 * ({@see \Protocol\Kafka\Protocol\Request\AddPartitionsToTxnResponse::resultOf()}).
 *
 * The inner structures did not change with the version: the topic entry is still
 * {@see AddPartitionsToTxnResponseTopic} with its `partition_errors`, and a partition that is part of the
 * transaction is still reported with the error code 0.
 *
 * @see docs/protocol/3.9.md, section "AddPartitionsToTxn API (key 24, v0 to v4)"
 */
class AddPartitionsToTxnResult implements BinarySchemaInterface
{
    /**
     * The transactional id this entry answers
     */
    public string $transactionalId;

    /**
     * Result of every requested topic of that transaction, indexed by the topic name
     *
     * @var array<string, AddPartitionsToTxnResponseTopic>
     */
    public array $topicResults = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'transactionalId' => BinarySchema::TYPE_STRING,
            'topicResults'    => ['topic' => AddPartitionsToTxnResponseTopic::class],
        ];
    }
}
