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

namespace Protocol\Kafka\Protocol\Data;

/**
 * ListOffsets answer DTO of the versions 1, 2 and 3
 *
 * The versions 1 (Kafka 0.10.1, KIP-79), 2 (Kafka 0.11, KIP-98) and 3 (Kafka 2.0, KIP-219) share one shape: the
 * isolation level of version 2 is a field of the *request body*, not of a partition entry, and version 3 changed
 * nothing on the wire at all. **Version 4 (Kafka 2.1, KIP-320)** is what added a leader epoch to both sides, see
 * {@see OffsetsResponseTopic}.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v6), a.k.a. ListOffset"
 */
final class OffsetsResponseTopicV1 extends OffsetsResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
