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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One configuration entry of a DescribeConfigs answer
 *
 * <pre>
 *   DescribeConfigsResponseConfigEntry => config_name config_value read_only is_default is_sensitive
 *     config_name  => STRING
 *     config_value => NULLABLE_STRING
 *     read_only    => BOOLEAN
 *     is_default   => BOOLEAN
 *     is_sensitive => BOOLEAN
 * </pre>
 *
 * The anonymous entry schema of `DESCRIBE_CONFIGS_RESPONSE_ENTITY_V0` in `Protocol.java` @ 0.11.0.3. Note that the
 * order on the wire is not the order of the Java constructor `DescribeConfigsResponse.ConfigEntry(name, value,
 * isSensitive, isDefault, isReadOnly)`.
 *
 * The three flags are computed by `AdminManager.describeConfigs` @ 0.11.0.3:
 *
 *  - `readOnly` is true for **every** entry of a broker resource, because a 0.11 broker cannot change its own
 *    configuration at runtime, and false for every entry of a topic;
 *  - `isDefault` says that the value was NOT written for this resource - for a topic, that the option is missing
 *    from its ZooKeeper node and the value shown is the one the broker-level default implies; for a broker, that
 *    the option is missing from its `server.properties`;
 *  - `isSensitive` is true for every option whose `ConfigDef.Type` is `PASSWORD`, and the **value of such an entry
 *    is always null on the wire**: `valueAsString = if (isSensitive) null else ...`.
 *
 * @see docs/protocol/0.11.0.md, section "DescribeConfigs API (key 32, v0)"
 */
class DescribeConfigsResponseConfigEntry implements BinarySchemaInterface
{
    /**
     * Name of the option, e.g. `retention.ms`
     */
    public string $configName;

    /**
     * Value of the option as text, null when the option is sensitive or has no value at all
     */
    public ?string $configValue;

    /**
     * Whether the option cannot be changed with the AlterConfigs api
     */
    public bool $readOnly;

    /**
     * Whether the value is the default one, i.e. nothing was configured for this resource
     */
    public bool $isDefault;

    /**
     * Whether the option holds a secret, in which case the broker never sends its value
     */
    public bool $isSensitive;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'configName'  => BinarySchema::TYPE_STRING,
            'configValue' => BinarySchema::TYPE_NULLABLE_STRING,
            'readOnly'    => BinarySchema::TYPE_BOOLEAN,
            'isDefault'   => BinarySchema::TYPE_BOOLEAN,
            'isSensitive' => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}
