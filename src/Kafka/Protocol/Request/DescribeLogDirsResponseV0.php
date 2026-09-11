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
 * DescribeLogDirs response of version 0 (Kafka 1.0), the frame of version 1 with a lower version field
 *
 * <pre>
 *   DescribeLogDirs Response (Version: 0) => throttle_time_ms [log_dirs]
 * </pre>
 *
 * `DESCRIBE_LOG_DIRS_RESPONSE_V1 = DESCRIBE_LOG_DIRS_RESPONSE_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see DescribeLogDirsResponse::getScheme()} follows.
 *
 * The answer of version 1 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 1 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0 and v1)"
 */
final class DescribeLogDirsResponseV0 extends DescribeLogDirsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
