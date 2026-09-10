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
 * One configuration entry of an AlterConfigs request (`CONFIG_ENTRY` in `Protocol.java` @ 0.11.0.3)
 *
 * <pre>
 *   AlterConfigsRequestConfigEntry => config_name config_value
 *     config_name  => STRING
 *     config_value => NULLABLE_STRING
 * </pre>
 *
 * `CONFIG_ENTRY` is shared with the `configs` array of a CreateTopics request, where this client calls the halves
 * `configKey` and `configValue` ({@see CreateTopicsRequestConfig}) - the names of `Protocol.java` @ 0.10.2.2, which
 * that api was written against. This DTO uses the 0.11 name `config_name`, because the AlterConfigs api never had
 * any other, and the value is the NULLABLE string the 0.11 schema declares.
 *
 * A **null** value is accepted by the schema but not by a 0.11.0.3 broker: `AdminManager.alterConfigs` copies every
 * entry into a `java.util.Properties` with `properties.setProperty(name, value)`, which throws a
 * `NullPointerException` on a null value - the resource is then answered with the error code -1 (Unknown) and the
 * message `null`. An option is reset to its default by LEAVING IT OUT of the request, not by sending a null.
 *
 * @see docs/protocol/1.1.md, section "AlterConfigs API (key 33, v0)"
 */
class AlterConfigsRequestConfigEntry implements BinarySchemaInterface
{
    /**
     * Name of the option, e.g. `retention.ms`
     */
    public string $configName;

    /**
     * Value of the option, as text
     */
    public ?string $configValue;

    public function __construct(string $configName, ?string $configValue)
    {
        $this->configName  = $configName;
        $this->configValue = $configValue;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'configName'  => BinarySchema::TYPE_STRING,
            'configValue' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
