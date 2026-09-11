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

namespace Protocol\Kafka\Protocol\Data;

/**
 * One configuration entry of a DescribeConfigs answer of the version 0 (Kafka 0.11, KIP-133)
 *
 * <pre>
 *   DescribeConfigsResponseConfigEntry (Version: 0) => config_name config_value read_only is_default is_sensitive
 *     config_name  => STRING
 *     config_value => NULLABLE_STRING
 *     read_only    => BOOLEAN
 *     is_default   => BOOLEAN
 *     is_sensitive => BOOLEAN
 * </pre>
 *
 * `DESCRIBE_CONFIGS_RESPONSE_ENTRY_V0` of `DescribeConfigsResponse.java` @ 1.1.1, unchanged since 0.11.0.3. The
 * entry carries an `is_default` boolean where version 1 carries the `config_source` int8, and it has no synonyms, so
 * this class only lowers the version constant that {@see DescribeConfigsResponseConfigEntry::getScheme()} follows.
 *
 * A 1.1 broker still answers this version, but it fills the boolean from the *source* it computed:
 * `is_default = (source == DEFAULT_CONFIG)`, so an option whose broker synonym stands in the `server.properties` is
 * not a default here although the resource itself set nothing - see the section of the document.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v4)"
 */
final class DescribeConfigsResponseConfigEntryV0 extends DescribeConfigsResponseConfigEntry
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
