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
 * Fetch response of version 16 (key 1)
 *
 * The answer of the leader discovery of KIP-951 (Kafka 3.7) and the frame of version 17 byte for byte:
 * `FetchResponse.json` @ 3.9.2 declares not a field for that version and its whole comment is "Version 17 no
 * changes to the response (KIP-853)", see {@see FetchResponse}. This class decodes the answers of a request that
 * asked with {@see FetchRequestV16}.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "The replica directory id of KIP-853 (v17)"
 */
final class FetchResponseV16 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 16;
}
