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
 * One topic-level configuration entry of a CreateTopics request (`CONFIG_ENTRY` in `Protocol.java` @ 0.11.0.3)
 *
 * <pre>
 *   CreateTopicsRequestConfig => ConfigKey ConfigValue
 *     ConfigKey   => string
 *     ConfigValue => nullable string
 * </pre>
 *
 * **The value is a `NULLABLE_STRING`, and it is one in every version of the api.** `CONFIG_ENTRY` is a single
 * shared schema object in `Protocol.java` @ 0.11.0.3 - `config_name` STRING, `config_value` NULLABLE_STRING - and
 * `SINGLE_CREATE_TOPIC_REQUEST_V1 = SINGLE_CREATE_TOPIC_REQUEST_V0`, which version 2 reuses again, so the topic
 * entry of the versions 0, 1 and 2 is byte for byte the same structure. Verified against the container: a
 * `ff ff` value is accepted by the parser of all three versions and none of them closes the connection.
 *
 * A client still must not send one. `AdminManager.createTopics` @ 0.11.0.3 copies the entries into a
 * `java.util.Properties`, whose `setProperty` throws a `NullPointerException` for a null value; the controller
 * catches it and answers **that topic** with the error code **-1** (`UNKNOWN`), creating nothing. The value of
 * every topic-level option therefore travels as the text that `kafka-topics.sh --config key=value` would have
 * written into ZooKeeper, and the broker validates it with `LogConfig.validate()` before it creates anything - an
 * unknown key or an unparsable value is answered with the error code 40 (`InvalidConfig`) for that topic, with the
 * message `Unknown topic config name: <key>` from version 1 of the answer on.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v5)"
 */
class CreateTopicsRequestConfig implements BinarySchemaInterface
{
    /**
     * Name of the topic-level option, e.g. `retention.ms`
     */
    public string $configKey;

    /**
     * Value of the option, as text; `null` is on the wire but is answered with the error code -1
     */
    public ?string $configValue;

    public function __construct(string $configKey, ?string $configValue)
    {
        $this->configKey   = $configKey;
        $this->configValue = $configValue;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'configKey'   => BinarySchema::TYPE_STRING,
            'configValue' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
