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
 * CreateTopics answer of version 5, the frame of version 6 with a lower version field
 *
 * The version 6 of KIP-599 adds no field to the answer; what it adds is the error code 89
 * `ThrottlingQuotaExceeded` that a topic entry may carry, and with it the `throttle_time_ms` the client has to wait
 * before it retries that topic.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v7)"
 */
final class CreateTopicsResponseV5 extends CreateTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
