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

use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\ConfigSource;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One configuration entry of a DescribeConfigs answer, version 1 (Kafka 1.1, KIP-226)
 *
 * <pre>
 *   DescribeConfigsResponseConfigEntry (Version: 1) => config_name config_value read_only config_source
 *                                                      is_sensitive [config_synonyms]
 *     config_name     => STRING
 *     config_value    => NULLABLE_STRING
 *     read_only       => BOOLEAN
 *     config_source   => INT8
 *     is_sensitive    => BOOLEAN
 *     config_synonyms => DescribeConfigsResponseConfigSynonym
 * </pre>
 *
 * `DESCRIBE_CONFIGS_RESPONSE_ENTRY_V1` of `DescribeConfigsResponse.java` @ 1.1.1. KIP-226 **replaced** the
 * `is_default` boolean of version 0 ({@see DescribeConfigsResponseConfigEntryV0}) by the `config_source` int8 in the
 * very same place and appended the synonyms of the option; everything else is the entry of version 0, in the same
 * order. Note that the order on the wire is not the order of the Java constructor
 * `DescribeConfigsResponse.ConfigEntry(name, value, source, isSensitive, isReadOnly, synonyms)`.
 *
 * The three properties `AdminManager` @ 1.1.1 computes:
 *
 *  - `configSource` is where the value comes from - {@see ConfigSource::TOPIC_CONFIG} for an option that stands in
 *    the ZooKeeper node of the topic, {@see ConfigSource::DYNAMIC_BROKER_CONFIG} for one that KIP-226 set on this
 *    broker, {@see ConfigSource::DYNAMIC_DEFAULT_BROKER_CONFIG} for one set for every broker of the cluster,
 *    {@see ConfigSource::STATIC_BROKER_CONFIG} for one that stands in the `server.properties`, and
 *    {@see ConfigSource::DEFAULT_CONFIG} for one nobody ever set. A version 0 answer says `is_default = (source ==
 *    DEFAULT_CONFIG)`, which is why an option whose *broker* synonym is configured is not a default any more;
 *  - `readOnly` is `!DynamicBrokerConfig.AllDynamicConfigs.contains(name)` for a broker entry - "cannot be changed
 *    at runtime", where a 0.11 broker reported every one of its own options as read-only - and false for a topic;
 *  - `isSensitive` is true for every option whose `ConfigDef.Type` is `PASSWORD`, and the **value of such an entry
 *    is always null on the wire**, in the entry as well as in every synonym of it.
 *
 * @see docs/protocol/1.1.md, section "DescribeConfigs API (key 32, v0 and v1)"
 */
class DescribeConfigsResponseConfigEntry implements BinarySchemaInterface
{
    /**
     * Version of the DescribeConfigs API that this DTO belongs to
     */
    public const int VERSION = 1;

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
     *
     * Only version 0 carries the flag; version 1 replaced it by {@see self::$configSource} and a client derives it
     * with {@see self::isDefault()}, which is what `DescribeConfigsResponse` @ 1.1.1 writes into a version 0 frame.
     *
     * @deprecated Version 0 of the API only
     */
    public bool $isDefault;

    /**
     * Where the value comes from, one of the {@see ConfigSource} constants
     *
     * @since Version 1 of protocol
     */
    public int $configSource;

    /**
     * Whether the option holds a secret, in which case the broker never sends its value
     */
    public bool $isSensitive;

    /**
     * Every place the broker looked for this value, the winning one first; empty for `include_synonyms = false`
     *
     * @since Version 1 of protocol
     *
     * @var list<DescribeConfigsResponseConfigSynonym>
     */
    public array $configSynonyms = [];

    /**
     * Returns where the value of this entry comes from, for both versions of the api
     *
     * A version 0 answer carries `is_default` alone, and `DescribeConfigsResponse(Struct)` @ 1.1.1 derives the
     * source of such an entry exactly like this: a default is {@see ConfigSource::DEFAULT_CONFIG}, and anything else
     * is the "configured" source of the resource type - {@see ConfigSource::TOPIC_CONFIG} for a topic,
     * {@see ConfigSource::STATIC_BROKER_CONFIG} for a broker.
     *
     * @param int $resourceType Type of the resource this entry belongs to, a `ConfigResource::TYPE_*` constant
     */
    public function source(int $resourceType): int
    {
        if (static::VERSION >= 1) {
            return ConfigSource::fromWire($this->configSource);
        }

        if ($this->isDefault) {
            return ConfigSource::DEFAULT_CONFIG;
        }

        return match ($resourceType) {
            ConfigResource::TYPE_TOPIC  => ConfigSource::TOPIC_CONFIG,
            ConfigResource::TYPE_BROKER => ConfigSource::STATIC_BROKER_CONFIG,
            default                     => ConfigSource::UNKNOWN,
        };
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'configName'  => BinarySchema::TYPE_STRING,
            'configValue' => BinarySchema::TYPE_NULLABLE_STRING,
            'readOnly'    => BinarySchema::TYPE_BOOLEAN,
        ];

        // KIP-226 replaced the `is_default` boolean by the `config_source` int8 in the very same place
        $scheme += static::VERSION >= 1
            ? ['configSource' => BinarySchema::TYPE_INT8]
            : ['isDefault' => BinarySchema::TYPE_BOOLEAN];

        $scheme['isSensitive'] = BinarySchema::TYPE_BOOLEAN;
        if (static::VERSION >= 1) {
            $scheme['configSynonyms'] = [DescribeConfigsResponseConfigSynonym::class];
        }

        return $scheme;
    }
}
