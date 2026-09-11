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
 * TxnOffsetCommit response object, version 2 (key 28)
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
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v3)"
 */
class TxnOffsetCommitResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

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
