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
 * DeleteTopics response of version 3 (Kafka 2.1), the body of version 4 in the encoding before KIP-482
 *
 * <pre>
 *   DeleteTopics Response (Version: 1 to 3) => throttle_time_ms [topic_error_codes]
 * </pre>
 *
 * The fields of the version 4 are these; what KIP-482 changed is how they are written - a compact array of compact
 * strings, and a tagged-field section at the end of the body and of every topic result.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v4)"
 */
final class DeleteTopicsResponseV3 extends DeleteTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
