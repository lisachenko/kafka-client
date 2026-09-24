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
 * Fetch response of version 17 (key 1)
 *
 * The answer of the version of KIP-853 (Kafka 3.9), the frame of versions 16 and 18 alike: `FetchResponse.json`
 * @ 4.1.0 comments "Version 18 no changes to the response (KIP-1166)", see {@see FetchResponse}. This class decodes
 * the answer of a request that asked with {@see FetchRequestV17}.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "The replica directory id of KIP-853 (v17)"
 */
final class FetchResponseV17 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 17;
}
