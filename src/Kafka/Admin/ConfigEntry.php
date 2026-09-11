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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseConfigEntry;

/**
 * One configuration option of a {@see Config}, as a DescribeConfigs answer describes it
 *
 * The fields are the ones of `org.apache.kafka.clients.admin.ConfigEntry` @ 1.1.1 - `name`, `value`, `source`,
 * `isSensitive`, `isReadOnly` and `synonyms` - and their meaning is the one `AdminManager.describeConfigs` gives
 * them:
 *
 *  - `value` is the option as text, and it is **null** for a sensitive option: the broker never sends the value of
 *    an option whose type is `PASSWORD`;
 *  - `source` is where the value comes from, one of the {@see ConfigSource} constants: the ZooKeeper node of the
 *    topic, a dynamic broker config of KIP-226, the `server.properties` of the broker, or nothing at all;
 *  - `isDefault` is `source === ConfigSource::DEFAULT_CONFIG`, exactly as `ConfigEntry.isDefault()` @ 1.1.1 derives
 *    it. It is therefore **not** "the resource did not set it": an option whose *broker* synonym stands in the
 *    `server.properties` has the source `STATIC_BROKER_CONFIG` and is not a default, although the topic set nothing;
 *  - `isReadOnly` means "cannot be changed with AlterConfigs": false for every option of a topic, and for a broker
 *    option `!DynamicBrokerConfig.AllDynamicConfigs.contains(name)` since KIP-226, where a 0.11 broker reported
 *    every one of its own options as read-only;
 *  - `synonyms` are the places the broker looked for the value, the winning one first, and the list is only filled
 *    when the request asked for them ({@see AdminClient::describeConfigs()} with `$includeSynonyms`).
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0, v1 and v2)"
 */
final class ConfigEntry
{
    /**
     * Whether the value is the built-in default, i.e. nothing configured it anywhere
     *
     * Derived from the source, as `ConfigEntry.isDefault()` @ 1.1.1 does it; a version 0 answer carries the very
     * same boolean on the wire, and a 1.1 broker fills it from the source it computed.
     */
    public readonly bool $isDefault;

    /**
     * @param string             $name        Name of the option, e.g. `retention.ms`
     * @param string|null        $value       Value of the option as text, null when it is sensitive
     * @param int                $source      Where the value comes from, a {@see ConfigSource} constant
     * @param bool               $isSensitive Whether the option holds a secret, whose value the broker never sends
     * @param bool               $isReadOnly  Whether the option cannot be changed with the AlterConfigs api
     * @param list<ConfigSynonym> $synonyms   Places the broker looked for the value, the winning one first
     */
    public function __construct(
        public readonly string $name,
        public readonly ?string $value,
        public readonly int $source = ConfigSource::UNKNOWN,
        public readonly bool $isSensitive = false,
        public readonly bool $isReadOnly = false,
        public readonly array $synonyms = []
    ) {
        $this->isDefault = $source === ConfigSource::DEFAULT_CONFIG;
    }

    /**
     * Builds the entry from the one of a DescribeConfigs answer
     *
     * The `$resourceType` is only needed for an answer of the version 0, which carries an `is_default` boolean
     * instead of the source: `DescribeConfigsResponse(Struct)` @ 1.1.1 turns that boolean back into a source with
     * the type of the resource, and {@see DescribeConfigsResponseConfigEntry::source()} does the same here.
     *
     * @param int $resourceType Type of the resource the entry belongs to, a `ConfigResource::TYPE_*` constant
     */
    public static function fromResponseEntry(
        DescribeConfigsResponseConfigEntry $entry,
        int $resourceType = ConfigResource::TYPE_UNKNOWN
    ): self {
        return new self(
            $entry->configName,
            $entry->configValue,
            $entry->source($resourceType),
            $entry->isSensitive,
            $entry->readOnly,
            array_map(ConfigSynonym::fromResponseSynonym(...), $entry->configSynonyms)
        );
    }
}
