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
 * OffsetFetch response, version 5: the committed offsets with their leader epochs, plainly encoded
 *
 * Version 6 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see OffsetFetchResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
final class OffsetFetchResponseV5 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
