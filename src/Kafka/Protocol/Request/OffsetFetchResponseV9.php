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
 * OffsetFetch answer of version 9 (Kafka 3.7, KIP-848): the last version that names its topics
 *
 * The answer of version 8 - "the response is the same as version 8 but can return STALE_MEMBER_EPOCH and
 * UNKNOWN_MEMBER_ID errors when the new consumer group protocol is used" (`OffsetFetchResponse.json` @ 3.7.2) -
 * whose topic entries carry the name. Version 10 (Kafka 4.2, KIP-848) names every topic by its id
 * ({@see OffsetFetchResponse}).
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The member id and epoch of KIP-848 (v9)"
 */
final class OffsetFetchResponseV9 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
