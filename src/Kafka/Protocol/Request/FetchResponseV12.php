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
 * Fetch response of version 12 (key 1)
 *
 * The last answer that names its topics by **name**: `FetchResponse.json` @ 3.1.2 declares the `Topic` of a topic
 * entry as `versions 0-12` and its `TopicId` as `13+`, so version 13 (Kafka 3.1, KIP-516) answers with the ids the
 * request carried and with nothing else, see {@see FetchResponse}. Everything else of the frame - the flexible
 * encoding, the top-level error code and session id of KIP-227 and the three tagged fields of a partition entry -
 * is the one of version 12.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v13)" and "The topic ids of the fetch path
 *      (v13, KIP-516)"
 */
final class FetchResponseV12 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
