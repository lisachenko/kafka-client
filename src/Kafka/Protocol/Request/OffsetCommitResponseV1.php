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
 * OffsetCommit response, version 1: the topic array alone, without the throttle time of version 3
 *
 * <pre>
 *   OffsetCommit Response (Version: 0, 1 and 2) => [responses]
 * </pre>
 *
 * Byte for byte the answer of {@see OffsetCommitResponseV2} and {@see OffsetCommitResponseV0}; the class exists so
 * that the version of an answer matches the version of the request that asked for it.
 *
 * @see docs/protocol/0.11.0.md, section "OffsetCommit API (key 8, v0 to v3)"
 */
final class OffsetCommitResponseV1 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
