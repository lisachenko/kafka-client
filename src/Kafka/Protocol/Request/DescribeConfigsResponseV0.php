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
 * DescribeConfigs response object of the version 0 (key 32, Kafka 0.11)
 *
 * <pre>
 *   DescribeConfigs Response (Version: 0) => throttle_time_ms [resources]
 *     resources => error_code error_message resource_type resource_name [config_entries]
 *       config_entries => config_name config_value read_only is_default is_sensitive
 * </pre>
 *
 * The answer of version 0 differs from version 1 in its **config entries** alone - an `is_default` boolean where
 * version 1 carries the `config_source` int8, and no synonyms - so this class only lowers the version constant that
 * {@see DescribeConfigsResponse::resourceClass()} follows.
 *
 * A 1.1.1 broker fills that boolean from the source it computed (`is_default = (source == DEFAULT_CONFIG)`), so the
 * answer of this version is not the answer a 0.11.0.3 broker gave: an option whose broker synonym stands in the
 * `server.properties` of the container is reported as NOT default here, where 0.11 reported it as one.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 and v1)"
 */
final class DescribeConfigsResponseV0 extends DescribeConfigsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
