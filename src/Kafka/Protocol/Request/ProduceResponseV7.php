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
 * Produce response object, version 7
 *
 * The frame of version 7 (Kafka 2.1) is the frame of version 5: the partition entries carry the base offset, the
 * log append time and the log start offset, and nothing else. Version 8 (Kafka 2.4, KIP-467) appended the
 * `record_errors` array and the `error_message` to every one of them, see {@see ProduceResponse}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v8)"
 */
final class ProduceResponseV7 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
