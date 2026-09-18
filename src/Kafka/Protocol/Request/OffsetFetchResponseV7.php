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
 * OffsetFetch answer of version 7 (Kafka 2.5, KIP-447): the topics and the group error at the top level
 *
 * Version 8 (Kafka 3.0) moved both of them into a `groups` array, one entry per group of the request, and left the
 * answer without a top-level error code; this version answers the one group its request named.
 * {@see OffsetFetchResponse} decodes the batched answer.
 *
 * @see docs/protocol/3.9.md, section "Stable offsets and the 88 of KIP-447 (Kafka 2.5)"
 * @see docs/protocol/3.9.md, section "OffsetFetch API (key 9, v0 to v8)"
 */
final class OffsetFetchResponseV7 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
