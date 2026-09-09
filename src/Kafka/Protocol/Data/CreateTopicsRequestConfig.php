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
 * One topic-level configuration entry of a CreateTopics request (`CONFIG_ENTRY` in `Protocol.java` @ 0.10.2.2)
 *
 * <pre>
 *   CreateTopicsRequestConfig => ConfigKey ConfigValue
 *     ConfigKey   => string
 *     ConfigValue => string
 * </pre>
 *
 * Both halves are plain, non-nullable strings: the value of every topic-level option travels as the text that
 * `kafka-topics.sh --config key=value` would have written into ZooKeeper, and the broker validates it with
 * `LogConfig.validate()` before it creates anything - an unknown key or an unparsable value is answered with the
 * error code 40 (InvalidConfig) for that topic.
 *
 * @see docs/protocol/0.11.0.md, section "CreateTopics API (key 19, v0 and v1)"
 */
class CreateTopicsRequestConfig implements BinarySchemaInterface
{
    /**
     * Name of the topic-level option, e.g. `retention.ms`
     */
    public string $configKey;

    /**
     * Value of the option, as text
     */
    public string $configValue;

    public function __construct(string $configKey, string $configValue)
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
            'configValue' => BinarySchema::TYPE_STRING,
        ];
    }
}
