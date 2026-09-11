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
 * Fetch response object (key 1), version 9
 *
 * <pre>
 *   FetchResponse (Version: 9) => ThrottleTimeMs ErrorCode SessionId
 *                                 [TopicName [Partition ErrorCode HighwaterMarkOffset LastStableOffset
 *                                             LogStartOffset [AbortedTransactions] RecordSetSize RecordSet]]
 * </pre>
 *
 * `FetchResponse.json` @ 2.8.2 has no field between version 7 and version 11, so this frame is the frame of
 * version 7 and of version 10 alike; what the version says is what the *request* promised, see
 * {@see FetchResponse}. An answer has to be read with the class of the version its request was sent with, which is
 * the only reason this class exists.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v10)" and "The leader epoch (KIP-320)"
 */
final class FetchResponseV9 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
