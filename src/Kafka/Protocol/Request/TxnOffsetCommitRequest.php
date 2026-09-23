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

use Protocol\Kafka\Consumer\ConsumerGroupMetadata;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopicV0;

/**
 * TxnOffsetCommit, version 5: commits consumer offsets inside a transaction (key 28, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   TxnOffsetCommit Request (Version: 2) => transactional_id consumer_group_id producer_id producer_epoch [topics]
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
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TXN_OFFSET_COMMIT_REQUEST_V1 =
 * TXN_OFFSET_COMMIT_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see TxnOffsetCommitRequestV0} is the same frame with the version field of Kafka 0.11.
 *
 * **Kafka 2.1 added version 2** (KIP-320), the first version of this api whose frame really changed: every
 * partition of it carries a `committed_leader_epoch` between the offset and the metadata
 * ({@see \Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestPartition}), so that the coordinator stores the
 * epoch of the leader the offset was read from next to the offset itself. It is the version this client sends;
 * {@see TxnOffsetCommitRequestV1} is the frame without that field, with the version of Kafka 2.0.
 *
 * **Kafka 2.5 added the version 3** (KIP-447), whose request carries **who the consumer is**: a `generation_id`,
 * a `member_id` and a nullable `group_instance_id` between the producer epoch and the topics. Until then the
 * coordinator only knew the group, so a consumer that had already been rebalanced away could still commit into a
 * transaction; now `GroupCoordinator.handleTxnCommitOffsets` @ 2.8.2 refuses a stale membership with **22**
 * `IllegalGeneration`, **25** `UnknownMemberId` or **82** `FencedInstanceId`. The generation **-1** with an empty
 * member id is the "not a member" form and is accepted exactly as a version 2 commit was, which is what
 * {@see ConsumerGroupMetadata::forGroup()} builds. The version 3 is also the first **flexible** one of this api.
 * {@see TxnOffsetCommitRequestV2} is the frame of Kafka 2.1.
 *
 * **Kafka 3.8 added the version 4** (KIP-890): *"Version 4 adds support for new error code TRANSACTION_ABORTABLE"*
 * (`TxnOffsetCommitRequest.json` @ 3.8.1), no field, the version 3 frame of KIP-447 unchanged.
 * {@see TxnOffsetCommitRequestV3} is the same frame with the version field of Kafka 2.5.
 *
 * **Kafka 4.0 added the version 5** (KIP-890 part 2): *"Version 5 is the same as version 4 (KIP-890). Note when
 * TxnOffsetCommit requests are used in transaction, if transaction V2 (KIP_890 part 2) is enabled, the
 * TxnOffsetCommit request will also include the function for a AddOffsetsToTxn call. If V2 is disabled, the client
 * can't use TxnOffsetCommit request version higher than 4 within a transaction."* (`TxnOffsetCommitRequest.json`
 * @ 4.0.0). The frame is the one of the version 4; the version is what the group coordinator acts on:
 * `AddPartitionsToTxnManager.txnOffsetCommitRequestVersionToTransactionSupportedOperation` @ 4.0.0 maps a version
 * above 4 to `addPartition`, so the `__consumer_offsets` partition of the group is **enrolled** into the open
 * transaction instead of merely verified - the {@see AddOffsetsToTxnRequest} the protocol v1 sends first is gone.
 * The Java `TxnOffsetCommitRequest.Builder` @ 4.0.0 caps the version at `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 =
 * 4` unless the cluster finalizes `transaction.version` 2; {@see TxnOffsetCommitRequestV4} is that frame, with the
 * version field of Kafka 3.8, and the one that still has an AddOffsetsToTxn in front of it.
 *
 * @see docs/protocol/4.3.md, section "TxnOffsetCommit API (key 28, v0 to v5)"
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
    public const int VERSION = 5;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Offsets to commit, indexed by the topic name
     *
     * @var array<string, TxnOffsetCommitRequestTopic>
     */
    protected readonly array $topics;

    /**
     * Generation of the consumer group the offsets belong to, -1 for a producer that is not a member
     *
     * @since Version 3 of protocol
     */
    protected int $generationId = OffsetCommitRequest::DEFAULT_GENERATION_ID;

    /**
     * Member id the coordinator assigned to that consumer, the empty string when there is none
     *
     * @since Version 3 of protocol
     */
    protected string $memberId = '';

    /**
     * `group.instance.id` of a static member (KIP-345), null for a dynamic one
     *
     * @since Version 3 of protocol
     */
    protected ?string $groupInstanceId = null;

    /**
     * @param string $transactionalId `transactional.id` of the producer that owns the transaction
     * @param string $groupId         Consumer group whose offsets are committed (`consumer_group_id` on the wire)
     * @param int    $producerId      Producer id the transaction coordinator handed out for that transactional id
     * @param int    $producerEpoch   Epoch of that producer id
     * @param array<string, array<int, int|OffsetAndMetadata>|TxnOffsetCommitRequestTopic> $topicPartitionOffsets
     *        Offsets to commit, as topic => partition => offset
     * @param ConsumerGroupMetadata|null $groupMetadata Who the consumer is inside its group (KIP-447, version 3);
     *        `null` is the "not a member" form, the generation -1 with an empty member id
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
        /**
         * Who the consumer is inside its group, `null` for a producer that is not a member of it
         *
         * @since Version 3 of protocol
         */
        protected readonly ?ConsumerGroupMetadata $groupMetadata = null,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $topicClass   = static::topicClass();
        $packedTopics = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            $packedTopics[$topic] = $partitionOffsets instanceof TxnOffsetCommitRequestTopic
                ? $partitionOffsets
                : new $topicClass((string) $topic, $partitionOffsets);
        }
        $this->topics = $packedTopics;

        if ($groupMetadata !== null) {
            $this->generationId    = $groupMetadata->generationId;
            $this->memberId        = $groupMetadata->memberId;
            $this->groupInstanceId = $groupMetadata->groupInstanceId;
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        $body = [
            'transactionalId' => BinarySchema::TYPE_STRING,
            'groupId'         => BinarySchema::TYPE_STRING,
            'producerId'      => BinarySchema::TYPE_INT64,
            'producerEpoch'   => BinarySchema::TYPE_INT16,
        ];
        if (static::VERSION >= 3) {
            $body['generationId']    = BinarySchema::TYPE_INT32;
            $body['memberId']        = BinarySchema::TYPE_STRING;
            $body['groupInstanceId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $body['topics'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this request belongs to
     *
     * @return class-string<TxnOffsetCommitRequestTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 2 ? TxnOffsetCommitRequestTopic::class : TxnOffsetCommitRequestTopicV0::class;
    }
}
