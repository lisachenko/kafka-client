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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One resource of an AlterConfigs request, i.e. one entry of the `resources` array
 *
 * <pre>
 *   AlterConfigsRequestResource => resource_type resource_name [config_entries]
 *     resource_type  => INT8
 *     resource_name  => STRING
 *     config_entries => config_name config_value
 * </pre>
 *
 * `ALTER_CONFIGS_REQUEST_RESOURCE_V0` in `Protocol.java` @ 0.11.0.3. The entries are the **complete** configuration
 * the resource should have afterwards, not a patch: `AdminManager.alterConfigs` builds a fresh `Properties` from
 * them and hands it to `AdminUtils.changeTopicConfig`, which REPLACES the ZooKeeper node of the topic. Every option
 * that is not in this array is therefore reset to its default.
 *
 * @see docs/protocol/2.8.md, section "AlterConfigs API (key 33, v0 to v2)"
 */
class AlterConfigsRequestResource implements BinarySchemaInterface
{
    /**
     * Type of the resource, one of the `ConfigResource::TYPE_*` constants
     */
    public int $resourceType;

    /**
     * Name of the resource: the topic name, or the broker id as a decimal string
     */
    public string $resourceName;

    /**
     * The whole configuration the resource should have, indexed by the option name
     *
     * @var array<string, AlterConfigsRequestConfigEntry>
     */
    public array $configEntries;

    /**
     * @param array<string, string|null|AlterConfigsRequestConfigEntry> $configEntries Complete configuration of the
     *        resource, as option name => value
     */
    public function __construct(int $resourceType, string $resourceName, array $configEntries)
    {
        $entries = [];
        foreach ($configEntries as $configName => $configValue) {
            $entries[$configName] = $configValue instanceof AlterConfigsRequestConfigEntry
                ? $configValue
                : new AlterConfigsRequestConfigEntry((string) $configName, $configValue);
        }

        $this->resourceType  = $resourceType;
        $this->resourceName  = $resourceName;
        $this->configEntries = $entries;
    }

    /**
     * Builds the wire entry of a resource the caller named with a {@see ConfigResource}
     *
     * @param array<string, string|null|AlterConfigsRequestConfigEntry> $configEntries Complete configuration
     */
    public static function fromConfigResource(ConfigResource $resource, array $configEntries): self
    {
        return new self($resource->type, $resource->name, $configEntries);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'resourceType'  => BinarySchema::TYPE_INT8,
            'resourceName'  => BinarySchema::TYPE_STRING,
            'configEntries' => ['configName' => AlterConfigsRequestConfigEntry::class],
        ];
    }
}
