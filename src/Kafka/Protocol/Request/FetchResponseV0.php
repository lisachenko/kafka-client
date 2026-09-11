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
 * Fetch response object (key 1), version 0
 *
 * <pre>
 *   FetchResponse (Version: 0) => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
 * </pre>
 *
 * The answer of a version 0 request has no `ThrottleTimeMs` prefix, so this class only lowers the version constant
 * that {@see FetchResponse::getScheme()} follows. Reading a version 0 answer with any higher version class would
 * take the size of the topics array for the throttle time and desynchronize the whole frame.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchResponseV0 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
