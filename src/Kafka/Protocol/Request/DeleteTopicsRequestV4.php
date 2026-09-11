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
 * DeleteTopics request of version 4, the frame of version 5 with a lower version field
 *
 * The request of the version 5 that Kafka 2.7 added is the same two fields; what the version changes is the
 * **answer** - it gains an `error_message` per topic - and the promise of KIP-599 that the client understands the
 * error code **89** `ThrottlingQuotaExceeded` and retries after the `throttle_time_ms` of the answer.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v5)"
 */
final class DeleteTopicsRequestV4 extends DeleteTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
