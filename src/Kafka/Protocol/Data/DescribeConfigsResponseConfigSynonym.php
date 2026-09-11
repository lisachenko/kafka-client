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

use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One synonym of a configuration entry of a DescribeConfigs answer, version 1 (Kafka 1.1, KIP-226)
 *
 * <pre>
 *   DescribeConfigsResponseConfigSynonym => config_name config_value config_source
 *     config_name   => STRING
 *     config_value  => NULLABLE_STRING
 *     config_source => INT8
 * </pre>
 *
 * `DESCRIBE_CONFIGS_RESPONSE_SYNONYM_V1` of `DescribeConfigsResponse.java` @ 1.1.1, i.e. the Java
 * `DescribeConfigsResponse.ConfigSynonym`. A synonym is one of the places the broker looked for the value of an
 * option, in the order it looked: the FIRST entry of the array is the one that won and therefore carries the value
 * of the entry itself, the ones behind it are shadowed by it.
 *
 * `AdminManager.configSynonyms()` @ 1.1.1 builds the list, and the name of a synonym is not necessarily the name of
 * the option: the synonyms of the topic option `retention.ms` are the broker options `log.retention.ms`,
 * `log.retention.minutes` and `log.retention.hours`, while the synonyms of a broker option are that same option name
 * once per source it could have come from (dynamic broker, dynamic default broker, static broker).
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v4)"
 */
class DescribeConfigsResponseConfigSynonym implements BinarySchemaInterface
{
    /**
     * Name of the option this value was read under, e.g. `log.retention.ms` for the topic option `retention.ms`
     */
    public string $configName;

    /**
     * Value of the option as text, null when the option is sensitive or has no value at all
     */
    public ?string $configValue;

    /**
     * Where this value comes from, one of the {@see ConfigSource} constants
     */
    public int $configSource;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'configName'   => BinarySchema::TYPE_STRING,
            'configValue'  => BinarySchema::TYPE_NULLABLE_STRING,
            'configSource' => BinarySchema::TYPE_INT8,
        ];
    }
}
