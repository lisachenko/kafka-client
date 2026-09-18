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
 * One transaction of an AddPartitionsToTxn request of version 4 (Kafka 3.5, KIP-890)
 *
 * <pre>
 *   AddPartitionsToTxnTransaction => transactional_id producer_id producer_epoch verify_only [topics]
 *     transactional_id => COMPACT_STRING
 *     producer_id      => INT64
 *     producer_epoch   => INT16
 *     verify_only      => BOOLEAN
 *     topics           => name [partitions]
 *       name       => COMPACT_STRING
 *       partitions => INT32
 * </pre>
 *
 * **Version 4 moved the four top-level fields of the request into an array of these entries**
 * (`AddPartitionsToTxnRequest.json` @ 3.5.2: *"Version 4 adds VerifyOnly field to check if partitions are already
 * in transaction and adds support to batch multiple transactions"*), so that one frame asks the coordinator about
 * several transactional ids at once - the batch a broker sends while it verifies the transactional batches of
 * many producers, not a frame a client builds. The three fields the versions 0 to 3 carried at the top level keep
 * their names here, and `verify_only` is the one field the version added.
 *
 * `verify_only = true` **asks** whether the partitions are part of the open transaction instead of adding them:
 * the coordinator writes nothing at all and answers the code 0 for a partition it holds and, on a 3.9.2 node, the
 * **120** (`TransactionAbortable`) for one it does not, see
 * {@see \Protocol\Kafka\Protocol\Request\AddPartitionsToTxnRequest}.
 *
 * @see docs/protocol/3.9.md, section "AddPartitionsToTxn API (key 24, v0 to v4)"
 */
class AddPartitionsToTxnTransaction implements BinarySchemaInterface
{
    /**
     * Partitions of this transaction, indexed by the topic name
     *
     * @var array<string, PartitionsForTopic>
     */
    public readonly array $topics;

    /**
     * @param string $transactionalId The transactional id this entry is about
     * @param int    $producerId      Producer id the coordinator handed out for that transactional id
     * @param int    $producerEpoch   Epoch of that producer id
     * @param array<string, list<int>|PartitionsForTopic> $topics Partitions of this transaction, per topic
     * @param bool   $verifyOnly      Whether the partitions are only checked instead of added
     */
    public function __construct(
        /**
         * The transactional id whose transaction the partitions belong to
         */
        public readonly string $transactionalId,
        /**
         * Current producer id in use by the transactional id
         */
        public readonly int $producerId,
        /**
         * Current epoch associated with the producer id
         */
        public readonly int $producerEpoch,
        array $topics = [],
        /**
         * Whether the request only asks if the partitions are in the transaction instead of adding them
         *
         * `false` adds them, which is what every version below 4 does; `true` is the verification KIP-890 gave the
         * partition leader, which answers without changing the state of the transaction at all.
         */
        public readonly bool $verifyOnly = false
    ) {
        $packedTopics = [];
        foreach ($topics as $topic => $partitions) {
            $packedTopics[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values(array_map(intval(...), $partitions)));
        }
        $this->topics = $packedTopics;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'transactionalId' => BinarySchema::TYPE_STRING,
            'producerId'      => BinarySchema::TYPE_INT64,
            'producerEpoch'   => BinarySchema::TYPE_INT16,
            'verifyOnly'      => BinarySchema::TYPE_BOOLEAN,
            'topics'          => ['topic' => PartitionsForTopic::class],
        ];
    }
}
