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
use Protocol\Kafka\Protocol\Data\OffsetCommitResponseTopic;

/**
 * Offset commit response object, version 3
 *
 * <pre>
 *   OffsetCommit Response (Version: 3) => throttle_time_ms [responses]
 *     throttle_time_ms => INT32     -- since version 3
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition error_code
 *         partition  => INT32
 *         error_code => INT16
 * </pre>
 *
 * The versions 0, 1 and 2 answer the topics array and nothing else - `OFFSET_COMMIT_RESPONSE_V1 =
 * OFFSET_COMMIT_RESPONSE_V2 = OFFSET_COMMIT_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3 - and version 3 (KIP-124,
 * Kafka 0.11) put a `throttle_time_ms` in front of it. {@see OffsetCommitResponseV2},
 * {@see OffsetCommitResponseV1} and {@see OffsetCommitResponseV0} lower the version constant this scheme follows.
 *
 * @see docs/protocol/0.11.0.md, sections "OffsetCommit API (key 8, v0 to v3)" and "Quotas and throttle time"
 */
class OffsetCommitResponse extends AbstractResponse
{
    /**
     * Version of the OffsetCommit API that this class decodes the answer of
     */
    public const int VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 3 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * List of topics with the result for each of their partitions
     *
     * @var array<string, OffsetCommitResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 3) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['topics'] = ['topic' => OffsetCommitResponseTopic::class];

        return $header + $body;
    }
}
