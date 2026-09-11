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
 * CreateTopics response of version 4 (Kafka 2.4), the answer before the fields of KIP-525
 *
 * <pre>
 *   CreateTopics Response (Version: 2, 3 and 4) => throttle_time_ms [topic_errors]
 * </pre>
 *
 * A topic result of this version ends after the error message: the partition count, the replication factor and the
 * configuration of the new topic arrive with the version 5 of KIP-525, and so does the flexible encoding of
 * KIP-482.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v5)"
 */
final class CreateTopicsResponseV4 extends CreateTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
