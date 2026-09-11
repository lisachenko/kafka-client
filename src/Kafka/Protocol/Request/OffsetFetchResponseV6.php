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
 * OffsetFetch answer of version 6 (Kafka 2.4, KIP-482): the flexible frame of the versions below KIP-447
 *
 * Version 7 (Kafka 2.5) changed no field of this layout at all - what it added is the error code **88**
 * (`UnstableOffsetCommit`) that a partition of it can carry when the request asked for stable offsets, which is
 * why the version exists at all: a client that sends it promises to understand the 88 and to retry.
 *
 * @see docs/protocol/2.8.md, section "Stable offsets and the 88 of KIP-447 (Kafka 2.5)"
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
final class OffsetFetchResponseV6 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
