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
 * ShareFetch answer of version 1 (Kafka 4.1, KIP-932)
 *
 * The very bytes of {@see ShareFetchResponse}: `ShareFetchResponse.json` @ 4.2.0 raised the versions to `1-2` and
 * added no field, so this class only lowers the version the answer of a {@see ShareFetchRequestV1} is read with.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1 and v2)"
 */
final class ShareFetchResponseV1 extends ShareFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
