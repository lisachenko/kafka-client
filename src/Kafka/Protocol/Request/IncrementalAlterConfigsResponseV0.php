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
 * IncrementalAlterConfigs response of version 0 (Kafka 2.3), the body of version 1 before KIP-482
 *
 * <pre>
 *   IncrementalAlterConfigs Response (Version: 0) => throttle_time_ms [responses]
 * </pre>
 *
 * The same fields as the version 1, written without the compact types and the tagged-field sections of KIP-482.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0 and v1)"
 */
final class IncrementalAlterConfigsResponseV0 extends IncrementalAlterConfigsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
