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
 * CreatePartitions response of version 0 (Kafka 1.0), the frame of version 1 with a lower version field
 *
 * <pre>
 *   CreatePartitions Response (Version: 0) => throttle_time_ms [topic_errors]
 * </pre>
 *
 `CREATE_PARTITIONS_RESPONSE_V1 = CREATE_PARTITIONS_RESPONSE_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see CreatePartitionsResponse::getScheme()} follows.
 *
 * The answer of version 1 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 1 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 and v1)"
 */
final class CreatePartitionsResponseV0 extends CreatePartitionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
