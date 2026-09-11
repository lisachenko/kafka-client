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

use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One resource of an IncrementalAlterConfigs request, i.e. one entry of the `resources` array
 *
 * <pre>
 *   AlterConfigsResource => resource_type resource_name [configs]
 *     resource_type => INT8
 *     resource_name => STRING
 *     configs       => name config_operation value
 * </pre>
 *
 * `AlterConfigsResource` of `IncrementalAlterConfigsRequest.json` @ 2.8.2. Unlike the resource of
 * {@see AlterConfigsRequestResource}, the array holds the changes the caller wants and nothing else: every option
 * that is not named keeps the value it has.
 *
 * The changes of one resource are validated and applied **together**: a resource whose array names the same option
 * twice is answered with the error code 42 and `Error due to duplicate config keys : retention.ms`, and one whose
 * changes do not validate leaves the whole resource untouched.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0)"
 */
class IncrementalAlterConfigsRequestResource implements BinarySchemaInterface
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
     * The changes this request asks for, in the order the caller gave them
     *
     * The array is a LIST and not a map: the broker refuses a resource that names an option twice anyway, and the
     * order is the one it applies them in.
     *
     * @var list<IncrementalAlterConfigsRequestAlterableConfig>
     */
    public array $configs;

    /**
     * @param list<AlterConfigOp|IncrementalAlterConfigsRequestAlterableConfig> $configs Changes of this resource
     */
    public function __construct(int $resourceType, string $resourceName, array $configs)
    {
        $entries = [];
        foreach ($configs as $config) {
            $entries[] = $config instanceof AlterConfigOp
                ? IncrementalAlterConfigsRequestAlterableConfig::fromAlterConfigOp($config)
                : $config;
        }

        $this->resourceType = $resourceType;
        $this->resourceName = $resourceName;
        $this->configs      = $entries;
    }

    /**
     * Builds the wire entry of a resource the caller named with a {@see ConfigResource}
     *
     * @param list<AlterConfigOp|IncrementalAlterConfigsRequestAlterableConfig> $configs Changes of this resource
     */
    public static function fromConfigResource(ConfigResource $resource, array $configs): self
    {
        return new self($resource->type, $resource->name, $configs);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'resourceType' => BinarySchema::TYPE_INT8,
            'resourceName' => BinarySchema::TYPE_STRING,
            'configs'      => [IncrementalAlterConfigsRequestAlterableConfig::class],
        ];
    }
}
