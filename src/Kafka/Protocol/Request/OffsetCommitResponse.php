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
 * Offset commit response object, version 9
 *
 * <pre>
 *   OffsetCommit Response (Version: 3 to 6) => throttle_time_ms [responses]
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
 * Kafka 0.11) put a `throttle_time_ms` in front of it. Version 4 (KIP-219, Kafka 2.0) did not touch the answer
 * either, it only changed *when* a throttled broker sends it. {@see OffsetCommitResponseV3},
 * {@see OffsetCommitResponseV2}, {@see OffsetCommitResponseV1} and {@see OffsetCommitResponseV0} lower the version
 * constant this scheme follows, and so do {@see OffsetCommitResponseV4} and {@see OffsetCommitResponseV5}: the
 * answer is one and the same layout from version 3 on, because neither KIP-211 nor KIP-320 touched it.
 *
 * **Version 9 (Kafka 3.6, KIP-848) does not touch it either** - "the response is the same as version 8" in
 * `OffsetCommitResponse.json` @ 3.6.2 - but it carries two error codes the versions below it cannot: the **69**
 * `GroupIdNotFound` of a group the coordinator does not know, which a version 8 answer reports as the **22**
 * `IllegalGeneration`, and the **113** `StaleMemberEpoch` of a member of a KIP-848 group whose member epoch is
 * behind the one the coordinator holds. {@see OffsetCommitResponseV8} keeps the version below it.
 *
 * @see docs/protocol/4.3.md, sections "OffsetCommit API (key 8, v0 to v9)" and "Quotas and throttle time"
 */
class OffsetCommitResponse extends AbstractResponse
{
    /**
     * Version of the OffsetCommit API that this class decodes the answer of
     */
    public const int VERSION = 9;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 8;

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
