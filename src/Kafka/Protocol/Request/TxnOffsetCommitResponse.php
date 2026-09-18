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
use Protocol\Kafka\Protocol\Data\TxnOffsetCommitResponseTopic;

/**
 * TxnOffsetCommit response object, version 4 (key 28)
 *
 * <pre>
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
 * @see docs/protocol/3.9.md, section "TxnOffsetCommit API (key 28, v0 to v4)"
 */
class TxnOffsetCommitResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every committed topic, indexed by the topic name
     *
     * @var array<string, TxnOffsetCommitResponseTopic>
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
            'topics'         => ['topic' => TxnOffsetCommitResponseTopic::class],
        ];
    }
}
