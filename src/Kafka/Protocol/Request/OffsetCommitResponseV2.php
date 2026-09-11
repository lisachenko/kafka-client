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

/**
 * OffsetCommit response, version 2: the topic array alone, without the throttle time of version 3
 *
 * <pre>
 *   OffsetCommit Response (Version: 0, 1 and 2) => [responses]
 * </pre>
 *
 * The response of the versions 0, 1 and 2 is one and the same layout - `OFFSET_COMMIT_RESPONSE_V1 =
 * OFFSET_COMMIT_RESPONSE_V2 = OFFSET_COMMIT_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3 - and version 3 (KIP-124,
 * Kafka 0.11) put a `throttle_time_ms` in front of it. This class and its two siblings
 * ({@see OffsetCommitResponseV1}, {@see OffsetCommitResponseV0}) only lower the version constant that
 * {@see OffsetCommitResponse::getScheme()} follows, so that the class of an answer always names the version of the
 * request that asked for it.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v3)"
 */
final class OffsetCommitResponseV2 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
