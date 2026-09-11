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
 * TxnOffsetCommit response object, version 0 (key 28)
 *
 * <pre>
 *   TxnOffsetCommit Response (Version: 0) => throttle_time_ms [topics]
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
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0)"
 */
class TxnOffsetCommitResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

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
