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
 * CreateTopics request of version 5, the frame of version 6 with a lower version field
 *
 * Kafka 2.7 added the version 6 with KIP-599 and changed not one byte of the request or of the answer: "Version 6
 * is identical to version 5 but may return a THROTTLING_QUOTA_EXCEEDED error" (`CreateTopicsRequest.json` @ 2.7.2).
 * The higher version is the client's promise that it understands the error code **89** and retries the topics it
 * names after the `throttle_time_ms` of the answer; a broker answers a client that sent the version 5 with the
 * old behaviour instead - it holds the request in the quota queue and answers it late.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v7)"
 */
final class CreateTopicsRequestV5 extends CreateTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
