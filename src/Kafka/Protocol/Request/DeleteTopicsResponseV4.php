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
 * DeleteTopics answer of version 4, the answer of version 5 without its per-topic error message
 *
 * Kafka 2.7 appended an `error_message` (a compact nullable string) to every topic result with the version 5
 * (`DeleteTopicsResponse.json` @ 2.7.2: "Version 5 adds ErrorMessage in the response and may return a
 * THROTTLING_QUOTA_EXCEEDED error"); the versions 4 and below end after the error code.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
final class DeleteTopicsResponseV4 extends DeleteTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
