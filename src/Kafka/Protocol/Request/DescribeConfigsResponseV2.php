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
 * DescribeConfigs response of version 2 (Kafka 2.0), the frame of version 3 WITHOUT the type and the documentation
 *
 * <pre>
 *   DescribeConfigs Response (Version: 1 and 2) => throttle_time_ms [resources]
 * </pre>
 *
 * The entries of this answer are {@see \Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntryV1}: KIP-569
 * put the `config_type` and the `documentation` of the version 3 behind the synonyms, and nothing below it carries
 * them.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v3)"
 */
final class DescribeConfigsResponseV2 extends DescribeConfigsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
