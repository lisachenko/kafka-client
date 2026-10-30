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

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponseTopic;
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponseTopicV0;

/**
 * TxnOffsetCommit response object, version 6 (key 28)
 *
 * <pre>
 *   TxnOffsetCommit Response (Version: 6) => throttle_time_ms [topics]
 *     topics => topic_id [partitions]      -- the topic id of KIP-1319 in place of the name
 *       topic_id => UUID
 *
 *   TxnOffsetCommit Response (Version: 0 to 2) => throttle_time_ms [topics]
 *     throttle_time_ms => INT32
 *     topics           => topic [partitions]
 *       topic      => STRING
 *       partitions => partition error_code
 *         partition  => INT32
 *         error_code => INT16
 * </pre>
 *
 * The api was born in Kafka 0.11, after KIP-124 made `throttle_time_ms` the first field of every new answer.
 *
 * As in {@see AddPartitionsToTxnResponse} there is **no top-level error code**: the state of the group and of the
 * transaction - 14, 15, 16, 47, 48 - is repeated on every partition of the answer, because
 * `GroupCoordinator.handleTxnCommitOffsets` @ 0.11.0.3 maps its single result over the requested partitions.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TXN_OFFSET_COMMIT_RESPONSE_V1 =
 * TXN_OFFSET_COMMIT_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see TxnOffsetCommitResponseV0} is the same frame with the version field of Kafka 0.11, and
 * {@see TxnOffsetCommitResponseV1} the one of Kafka 2.0: the `committed_leader_epoch` that Kafka 2.1 added with
 * version 2 is a field of the REQUEST alone, so the three answers are one and the same frame.
 *
 * **Kafka 2.5 added the version 3** (KIP-447) and gave the answer no field: the generation, the member id and the
 * group instance id are in the REQUEST, and what the answer gains is three more error codes per partition - 22
 * `IllegalGeneration`, 25 `UnknownMemberId` and 82 `FencedInstanceId`. The version 3 is the first flexible one of
 * this api; {@see TxnOffsetCommitResponseV2} is the frame of Kafka 2.1.
 *
 * **Kafka 3.8 added the version 4** (KIP-890) and gave the answer no field either. The per-partition codes stay
 * the three of KIP-447 - a 3.9.2 group coordinator checks the member id before the generation, so an unknown
 * member is the **25** and the 22 needs a member the group really holds. **This is the one client-facing api
 * of the five whose refusal the version really changes**: the group coordinator verifies the
 * `__consumer_offsets` partition of the group against the transaction coordinator before it writes the
 * offset (the partition verification of KIP-890 part 1), and
 * `GroupCoordinator.handleTxnCommitOffsets` @ 3.9.2 picks
 * `transactionSupportedOperation = if (apiVersion >= 4) genericError else defaultError`, so a commit whose
 * partition is not part of the open transaction - no AddOffsetsToTxn preceded it - is answered the **120**
 * `TransactionAbortable` at the version 4 where the version 3 is answered the **48** `InvalidTxnState`.
 * {@see TxnOffsetCommitResponseV3} is the frame of Kafka 2.5, and the one that still gets the 48.
 *
 * **Kafka 4.0 added the version 5** (KIP-890 part 2): *"Version 5 is the same with version 3 (KIP-890)"*
 * (`TxnOffsetCommitResponse.json` @ 4.0.0) - no field. What changes is the request behind it: a version 5 enrols
 * the `__consumer_offsets` partition of the group into the transaction of the protocol v2 itself, so the commit
 * that the version 4 is refused the **120** for - no AddOffsetsToTxn in front of it - is answered **0** at the
 * version 5. {@see TxnOffsetCommitResponseV4} is the answer of Kafka 3.8.
 *
 * **Kafka 4.4 added the version 6** (KIP-1319): *"Version 6 adds support for topic IDs and removes support for topic
 * names. It can also return GROUP_ID_NOT_FOUND, STALE_MEMBER_EPOCH and UNKNOWN_TOPIC_ID"*
 * (`TxnOffsetCommitResponse.json` @ 4.4.0, `Name` `"0-5"`, `TopicId` `"6+"`), so {@see self::$topics} is a list of
 * entries that carry the `topic_id` and the empty name ({@see TxnOffsetCommitResponseTopic}). A partition of an id the
 * node does not know carries the **100** `UNKNOWN_TOPIC_ID`; a group the coordinator does not know (with a generation
 * of 0 or more) is the **69** `GroupIdNotFound` and a member epoch of KIP-848 the coordinator does not accept the
 * **113** `StaleMemberEpoch` - both of which a version 5 answer reports as the 22 `IllegalGeneration`.
 * {@see self::topicsByName()} maps the entries back to the names the request committed for;
 * {@see TxnOffsetCommitResponseV5} keeps the answer that names its topics.
 *
 * @see docs/protocol/overview.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 * @see docs/protocol/overview.md, section "What Kafka 4.4 changed here (KIP-1319): the version 6"
 */
class TxnOffsetCommitResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every committed topic, indexed by the topic name - a list from version 6 on, whose entries name the
     * topic by its id
     *
     * @var array<array-key, TxnOffsetCommitResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            // From version 6 the entries carry no name, so there is no field to index the array by
            'topics'         => static::VERSION >= TxnOffsetCommitRequest::MIN_TOPIC_ID_VERSION
                ? [TxnOffsetCommitResponseTopic::class]
                : ['topic' => TxnOffsetCommitResponseTopicV0::class],
        ];
    }

    /**
     * Returns the topics of the answer indexed by their name, whatever version the answer is
     *
     * An answer below version 6 is indexed by name already. An entry of a version 6 answer is named with the map of
     * the request - the names and the ids this client committed with - and an id the map does not hold, which a node
     * never answers, keeps the text form of its uuid as its name.
     *
     * @param array<string, string> $topicIds Id of every topic of the request, as name => the 16 raw bytes of its uuid
     *
     * @return array<string, TxnOffsetCommitResponseTopic>
     */
    public function topicsByName(array $topicIds = []): array
    {
        if (static::VERSION < TxnOffsetCommitRequest::MIN_TOPIC_ID_VERSION) {
            return $this->topics;
        }

        $namesById = array_flip($topicIds);
        $topics    = [];
        foreach ($this->topics as $topic) {
            if ($topic->topic === '') {
                $topic->topic = (string) ($namesById[$topic->topicId] ?? Uuid::toString($topic->topicId));
            }
            $topics[$topic->topic] = $topic;
        }

        return $topics;
    }
}
