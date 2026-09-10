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

use Protocol\Kafka\Protocol\Data\DescribeConfigsResponseResource;

/**
 * The complete configuration of one resource, as {@see AdminClient::describeConfigs()} read it
 *
 * `org.apache.kafka.clients.admin.Config` @ 0.11.0.3 keeps a plain collection of entries and looks an option up by
 * walking it; this class indexes the entries by their name instead, because an option name is unique within a
 * resource and a caller almost always wants one option out of the ~40 a topic has.
 *
 * <code>
 *   $configs = $admin->describeConfigs([ConfigResource::topic('events')]);
 *   $configs[ConfigResource::topic('events')->key()]->get('retention.ms')?->value;
 * </code>
 *
 * @see docs/protocol/1.1.md, section "DescribeConfigs API (key 32, v0)"
 */
final class Config
{
    /**
     * @param ConfigResource            $resource The resource this configuration belongs to
     * @param array<string, ConfigEntry> $entries Options of the resource, indexed by the option name
     */
    public function __construct(
        public readonly ConfigResource $resource,
        public readonly array $entries = []
    ) {}

    /**
     * Builds the configuration from the resource entry of a DescribeConfigs answer
     */
    public static function fromResponseResource(DescribeConfigsResponseResource $resource): self
    {
        $entries = [];
        foreach ($resource->configEntries as $entry) {
            $entries[$entry->configName] = ConfigEntry::fromResponseEntry($entry);
        }

        return new self(ConfigResource::fromWire($resource->resourceType, $resource->resourceName), $entries);
    }

    /**
     * Returns the entry of the given option, or null when the resource has no such option
     */
    public function get(string $name): ?ConfigEntry
    {
        return $this->entries[$name] ?? null;
    }

    /**
     * Returns the value of the given option, or null when it has none - which a sensitive option never has
     */
    public function value(string $name): ?string
    {
        return ($this->entries[$name] ?? null)?->value;
    }

    /**
     * Returns the options that were configured for this resource, i.e. everything that is not a default
     *
     * That is the set an AlterConfigs request has to send back to keep the resource as it is: the api replaces the
     * whole configuration, and every default is re-derived by the broker.
     *
     * @return array<string, string|null> Option name => value
     */
    public function nonDefaultValues(): array
    {
        $values = [];
        foreach ($this->entries as $name => $entry) {
            if (!$entry->isDefault) {
                $values[$name] = $entry->value;
            }
        }

        return $values;
    }
}
