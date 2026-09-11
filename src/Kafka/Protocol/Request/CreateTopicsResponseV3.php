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
 * CreateTopics response of version 3 (Kafka 2.0), the frame of version 4 with a lower version field
 *
 * <pre>
 *   CreateTopics Response (Version: 2, 3 and 4) => throttle_time_ms [topic_errors]
 * </pre>
 *
 * `CreateTopicsResponse.json` @ 2.8.2 gives the version 4 no field of its own - the note "Version 4 makes
 * partitions/replicationFactor optional even when assignments are not present (KIP-464)" is about the REQUEST -
 * and the next field of the answer, the `topic_configs` of KIP-525, arrives with the flexible version 5. So the
 * answer of version 4 is this frame, and this class only lowers the version constant that
 * {@see CreateTopicsResponse::getScheme()} follows.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v7)"
 */
final class CreateTopicsResponseV3 extends CreateTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
