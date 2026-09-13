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
 * OffsetCommit response, version 7: the throttle time and the topics, plainly encoded
 *
 * Version 8 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see OffsetCommitResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v8)"
 */
final class OffsetCommitResponseV7 extends OffsetCommitResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
