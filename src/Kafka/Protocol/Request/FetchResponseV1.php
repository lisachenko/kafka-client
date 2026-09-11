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
 * Fetch response object (key 1), version 1
 *
 * <pre>
 *   FetchResponse (Version: 1) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            MessageSetSize MessageSet]]
 * </pre>
 *
 * The frame of a version 1 answer is the one of the versions 2 and 3, so this class only lowers the version
 * constant to the version of the request it belongs to. What a version 1 answer does carry is a message set that
 * the broker converted down to message format v0, whatever the log holds, see {@see FetchRequestV1}.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v7)"
 */
final class FetchResponseV1 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
