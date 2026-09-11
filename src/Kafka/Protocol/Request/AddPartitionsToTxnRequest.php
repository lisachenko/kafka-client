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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\PartitionsForTopic;

/**
 * AddPartitionsToTxn, version 0: enrols topic-partitions into the open transaction (ApiKey 24, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   AddPartitionsToTxn Request (Version: 0) => transactional_id producer_id producer_epoch [topics]
 *     transactional_id => STRING
 *     producer_id      => INT64
 *     producer_epoch   => INT16
 *     topics           => topic [partitions]
 *       topic      => STRING
 *       partitions => INT32
 * </pre>
 *
 * The request has to reach the **transaction coordinator** of the transactional id
 * ({@see \Protocol\Kafka\Client::getTransactionCoordinator()}) and it has to be answered **before the first Produce
 * request** that writes into one of the partitions it names. It is what makes a transaction more than a flag on a
 * record batch: the coordinator writes the partition into the `__transaction_state` entry of the id, and that list
 * is the one it walks when the transaction ends, to write a COMMIT or an ABORT marker into every partition of it. A
 * partition that was never added would receive no marker, so a 0.11.0.3 broker refuses a *transactional* batch of a
 * partition that is not part of the open transaction with the error code **48** (`InvalidTxnState`).
 *
 * The first call of a transaction is also what *starts* it on the broker:
 * `TransactionCoordinator.handleAddPartitionsToTransaction` @ 0.11.0.3 moves the id from `Empty`, `CompleteCommit`
 * or `CompleteAbort` into `Ongoing` and stamps it with the start time that `transaction.timeout.ms` is counted
 * from. There is no "BeginTransaction" request in the protocol at all -
 * {@see \Protocol\Kafka\Producer\KafkaProducer::beginTransaction()} is a purely client-side state change.
 *
 * A partition may be added twice; the coordinator answers the error code 0 for one it already holds. Adding a
 * partition while the previous transaction of the id is still being completed is answered with **51**
 * (`ConcurrentTransactions`), which is retriable after a back-off.
 *
 * @see docs/protocol/2.8.md, section "AddPartitionsToTxn API (key 24, v0)"
 */
class AddPartitionsToTxnRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ADD_PARTITIONS_TO_TXN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Partitions to enrol into the transaction, indexed by the topic name
     *
     * @var array<string, PartitionsForTopic>
     */
    protected readonly array $topics;

    /**
     * @param string $transactionalId `transactional.id` of the producer that owns the transaction
     * @param int    $producerId      Producer id the coordinator handed out for that transactional id
     * @param int    $producerEpoch   Epoch of that producer id, which fences every older incarnation
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions Partitions to add, as topic => partitions
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * The transactional id whose transaction the partitions belong to
         */
        protected readonly string $transactionalId,
        /**
         * Current producer id in use by the transactional id
         */
        protected readonly int $producerId,
        /**
         * Current epoch associated with the producer id
         */
        protected readonly int $producerEpoch,
        array $topicPartitions = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopics = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopics[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values(array_map(intval(...), $partitions)));
        }
        $this->topics = $packedTopics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'transactionalId' => BinarySchema::TYPE_STRING,
            'producerId'      => BinarySchema::TYPE_INT64,
            'producerEpoch'   => BinarySchema::TYPE_INT16,
            'topics'          => ['topic' => PartitionsForTopic::class],
        ];
    }
}
