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
 * Fetch response object (key 1), version 2
 *
 * <pre>
 *   FetchResponse (Version: 2) => ThrottleTimeMs [TopicName [Partition ErrorCode HighwaterMarkOffset
 *                                                            MessageSetSize MessageSet]]
 * </pre>
 *
 * The frame of a version 2 answer is the frame of a version 1 and of a version 3 answer, so this class only lowers
 * the version constant to the version of the request it belongs to. Its message sets are the ones the log holds:
 * from version 2 on the broker no longer converts them down to message format v0, see {@see FetchRequestV2}.
 *
 * @see docs/protocol/0.10.2.md, section "Fetch API (key 1, v0 to v3)"
 */
final class FetchResponseV2 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
