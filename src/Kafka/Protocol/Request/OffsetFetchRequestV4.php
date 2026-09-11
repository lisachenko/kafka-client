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
 * OffsetFetch request of version 4 (Kafka 2.0, KIP-219), the frame of version 2 with a higher version field
 *
 * <pre>
 *   OffsetFetch Request (Version: 2 to 5) => group_id [topics]
 * </pre>
 *
 * The request of this api did not change between the nullable topic array of version 2 and the `require_stable`
 * of version 7; version 5 (Kafka 2.1, KIP-320) only added the `committed_leader_epoch` to the ANSWER, which is
 * why the version 4 request needs the answer class {@see OffsetFetchResponseV4} while
 * {@see OffsetFetchRequest} is read with {@see OffsetFetchResponse}.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
final class OffsetFetchRequestV4 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
