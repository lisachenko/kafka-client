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
 * OffsetFetch request of version 6 (Kafka 2.4, KIP-482): the flexible frame without the flag of KIP-447
 *
 * Version 7 (Kafka 2.5) appended the boolean `require_stable` behind the topic array, with which a consumer asks
 * the coordinator to hold back an offset whose transaction is still open. This version always reads the offset of
 * the last commit, pending or not.
 *
 * @see docs/protocol/2.8.md, section "Stable offsets and the 88 of KIP-447 (Kafka 2.5)"
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
final class OffsetFetchRequestV6 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
