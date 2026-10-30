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

use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Consumer\ConsumerGroupMetadata;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopicV0;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitRequestTopicV2;

/**
 * TxnOffsetCommit, version 6: commits consumer offsets inside a transaction (key 28, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   TxnOffsetCommit Request (Version: 6) => transactional_id consumer_group_id producer_id producer_epoch
 *                                           generation_id_or_member_epoch member_id group_instance_id [topics]
 *     topics => topic_id [partitions]      -- the topic id of KIP-1319 in place of the name
 *       topic_id => UUID
 *
 *   TxnOffsetCommit Request (Version: 3 to 5) => ... generation_id member_id group_instance_id [topics]
 *     generation_id     => INT32
 *     member_id         => STRING
 *     group_instance_id => NULLABLE_STRING
 *
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
 * epoch of the leader the offset was read from next to the offset itself. {@see TxnOffsetCommitRequestV1} is the
 * frame without that field, with the version of Kafka 2.0.
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
 * **Kafka 4.4 added the version 6** (KIP-1319), and this class is version 6: *"Version 6 adds support for topic IDs
 * and removes support for topic names"* (`TxnOffsetCommitRequest.json` @ 4.4.0) - the `Name` of a topic entry is
 * `"versions": "0-5"`, the new `TopicId` `"6+"` - the TxnOffsetCommit counterpart of what OffsetCommit v10 did in
 * Kafka 4.2. So a client can not commit for a topic whose id it does not know: the `$topicIds` of the constructor are
 * where it states them ({@see \Protocol\Kafka\Common\Cluster::topicIdsOf()} is where it learns them), and a version
 * 6 request without the id of one of its topics is refused before it is built, with {@see UnknownTopicIdException},
 * as the Java `TxnOffsetCommitRequest.Builder.build()` @ 4.4.0 refuses it with an `UnsupportedVersionException`. The
 * node answers a topic id it does not know with the **100** `UNKNOWN_TOPIC_ID` on every partition of it. The same
 * release renamed `generation_id` to `generation_id_or_member_epoch` - "the generation of the group if using the
 * classic group protocol or the member epoch if using the consumer protocol" - the same four bytes, so
 * {@see self::$generationId} keeps its published name. And the answer of the version 6 reports the **69**
 * `GroupIdNotFound` of a group the coordinator does not know and the **113** `StaleMemberEpoch` of a KIP-848 member
 * that commits with an epoch the coordinator does not accept, which `OffsetMetadataManager` @ 4.4.0 folds into the
 * **22** `IllegalGeneration` for every version below it. The version belongs to the transaction protocol **v2**, as
 * the version 5 does: the Java `Builder.forTopicIdsOrNames()` @ 4.4.0 caps it at 4 without `transaction.version` 2.
 * {@see TxnOffsetCommitRequestV5} keeps the version that names the topics, which a client sends for a topic whose id
 * it does not know (`Builder.forTopicNames()` @ 4.4.0).
 *
 * @see docs/protocol/overview.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 * @see docs/protocol/overview.md, section "What Kafka 4.4 changed here (KIP-1319): the version 6"
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
    public const int VERSION = 6;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * The first version that names a topic by its id, and by nothing else (Kafka 4.4, KIP-1319)
     */
    public const int MIN_TOPIC_ID_VERSION = 6;

    /**
     * Offsets to commit, indexed by the topic name - a list from version 6 on, whose entries carry no name on the wire
     *
     * @var array<array-key, TxnOffsetCommitRequestTopic>
     */
    protected readonly array $topics;

    /**
     * Id of every topic this request names, as topic name => the 16 raw bytes of its uuid (KIP-1319)
     *
     * Version 6 names every topic by its id and by nothing else; every version below it ignores the map.
     *
     * @var array<string, string>
     */
    protected readonly array $topicIds;

    /**
     * Generation of the consumer group the offsets belong to, -1 for a producer that is not a member
     *
     * `generation_id_or_member_epoch` from Kafka 4.4 on (KIP-1319): the generation of a classic member, the member
     * epoch of a member of the consumer protocol of KIP-848 - which is what
     * {@see \Protocol\Kafka\Consumer\KafkaConsumer::groupMetadata()} puts into it for one, as the Java consumer does.
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
     * @param array<string, string> $topicIds Id of every topic, as name => the 16 raw bytes of its uuid; **version 6
     *        needs one per topic** (KIP-1319), every lower version ignores the map
     *
     * @throws UnknownTopicIdException If a version 6 request names a topic whose id the caller did not state
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
        int $correlationId = 0,
        array $topicIds = []
    ) {
        $this->topicIds = $topicIds;
        $topicClass     = static::topicClass();
        $packedTopics   = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            $topic = $partitionOffsets instanceof TxnOffsetCommitRequestTopic
                ? $partitionOffsets->topic
                : (string) $topic;
            if (static::VERSION >= self::MIN_TOPIC_ID_VERSION) {
                // A version 6 entry carries no name at all, so the list it travels in is the only honest shape
                $packedTopics[] = new $topicClass(
                    $topic,
                    $partitionOffsets instanceof TxnOffsetCommitRequestTopic
                        ? $partitionOffsets->partitions
                        : $partitionOffsets,
                    self::idOf($topicIds, $topic, $partitionOffsets)
                );
                continue;
            }
            $packedTopics[$topic] = match (true) {
                $partitionOffsets instanceof $topicClass => $partitionOffsets,
                // An entry built for another version is repacked; its partitions are kept as they are
                $partitionOffsets instanceof TxnOffsetCommitRequestTopic => new $topicClass(
                    $topic,
                    $partitionOffsets->partitions
                ),
                default => new $topicClass($topic, $partitionOffsets),
            };
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
        // From version 6 the entries carry no name, so there is no field to index the array by
        $body['topics'] = static::VERSION >= self::MIN_TOPIC_ID_VERSION
            ? [static::topicClass()]
            : ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the id of every topic this request names, as topic name => the 16 raw bytes of its uuid
     *
     * @return array<string, string>
     */
    public function getTopicIds(): array
    {
        return $this->topicIds;
    }

    /**
     * Returns the id of a topic from the map of the caller, or from a ready-made entry that carries one
     *
     * A version below 6 names its topics by name and never looks at the map; a version 6 frame can not name a topic
     * at all without its id, and a client that does not know it sends the version 5 instead of guessing.
     *
     * @param array<string, string> $topicIds Id of every topic, as name => the 16 raw bytes of its uuid
     * @param mixed                 $entry    What the caller gave for the topic
     *
     * @throws UnknownTopicIdException If the id of the topic is not known
     */
    private static function idOf(array $topicIds, string $topic, mixed $entry): string
    {
        $topicId = $topicIds[$topic] ?? Uuid::ZERO;
        if (Uuid::isZero($topicId) && $entry instanceof TxnOffsetCommitRequestTopic) {
            $topicId = $entry->topicId;
        }
        if (Uuid::isZero($topicId)) {
            throw new UnknownTopicIdException(
                [
                    'error' => 'A TxnOffsetCommit request of version 6 names its topics by id (KIP-1319), and this'
                        . ' client does not know the id of this one',
                    'topic' => $topic,
                ]
            );
        }

        return $topicId;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this request belongs to
     *
     * @return class-string<TxnOffsetCommitRequestTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= self::MIN_TOPIC_ID_VERSION => TxnOffsetCommitRequestTopic::class,
            static::VERSION >= 2                          => TxnOffsetCommitRequestTopicV2::class,
            default                                       => TxnOffsetCommitRequestTopicV0::class,
        };
    }
}
