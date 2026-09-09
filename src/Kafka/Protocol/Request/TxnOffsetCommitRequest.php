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

use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopic;

/**
 * TxnOffsetCommit, version 0: commits consumer offsets inside a transaction (key 28, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   TxnOffsetCommit Request (Version: 0) => transactional_id consumer_group_id producer_id producer_epoch [topics]
 *     transactional_id  => STRING
 *     consumer_group_id => STRING
 *     producer_id       => INT64
 *     producer_epoch    => INT16
 *     topics            => topic [partitions]
 *       topic      => STRING
 *       partitions => partition offset metadata
 *         partition => INT32
 *         offset    => INT64
 *         metadata  => NULLABLE_STRING
 * </pre>
 *
 * The second half of `sendOffsetsToTransaction()`, and **the one request of the transaction protocol that goes to
 * the group coordinator** ({@see \Protocol\Kafka\Client::getGroupCoordinator()}) instead of to the transaction
 * coordinator, because it is the group coordinator that owns `__consumer_offsets`. It has to follow an
 * {@see AddOffsetsToTxnRequest} for the same group, which is what puts that partition of `__consumer_offsets` into
 * the transaction; without it the group coordinator answers **48** (`InvalidTxnState`), the code of a transactional
 * write into a partition that is not part of an open transaction.
 *
 * What the group coordinator writes is an ordinary offset commit **inside a transactional record batch** stamped
 * with the producer id and the epoch of this request. Those records are invisible to an OffsetFetch of a
 * `read_committed` reader - and to the group itself - until the transaction coordinator has written the COMMIT
 * marker into that partition; an aborted transaction leaves them in the log with an ABORT marker behind them and
 * the group keeps the offset it had before.
 *
 * Unlike {@see OffsetCommitRequest} this api carries **no generation id and no member id**: the producer that
 * commits the offsets is not a member of the group, and the fencing that a generation would give is done by the
 * producer epoch instead. It has no `retention_time` either.
 *
 * @see docs/protocol/0.11.0.md, section "TxnOffsetCommit API (key 28, v0)"
 */
class TxnOffsetCommitRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::TXN_OFFSET_COMMIT;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Offsets to commit, indexed by the topic name
     *
     * @var array<string, TxnOffsetCommitRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string $transactionalId `transactional.id` of the producer that owns the transaction
     * @param string $groupId         Consumer group whose offsets are committed (`consumer_group_id` on the wire)
     * @param int    $producerId      Producer id the transaction coordinator handed out for that transactional id
     * @param int    $producerEpoch   Epoch of that producer id
     * @param array<string, array<int, int|OffsetAndMetadata>|TxnOffsetCommitRequestTopic> $topicPartitionOffsets
     *        Offsets to commit, as topic => partition => offset
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * The transactional id whose transaction the offsets belong to
         */
        protected readonly string $transactionalId,
        /**
         * Id of the associated consumer group to commit offsets for
         */
        protected readonly string $groupId,
        /**
         * Current producer id in use by the transactional id
         */
        protected readonly int $producerId,
        /**
         * Current epoch associated with the producer id
         */
        protected readonly int $producerEpoch,
        array $topicPartitionOffsets = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopics = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            $packedTopics[$topic] = $partitionOffsets instanceof TxnOffsetCommitRequestTopic
                ? $partitionOffsets
                : new TxnOffsetCommitRequestTopic((string) $topic, $partitionOffsets);
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
            'groupId'         => BinarySchema::TYPE_STRING,
            'producerId'      => BinarySchema::TYPE_INT64,
            'producerEpoch'   => BinarySchema::TYPE_INT16,
            'topics'          => ['topic' => TxnOffsetCommitRequestTopic::class],
        ];
    }
}
