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
 * One resource of a DescribeConfigs request, i.e. one entry of the `resources` array
 *
 * <pre>
 *   DescribeConfigsRequestResource => resource_type resource_name [config_names]
 *     resource_type => INT8
 *     resource_name => STRING
 *     config_names  => NULLABLE_ARRAY of STRING
 * </pre>
 *
 * `DESCRIBE_CONFIGS_REQUEST_RESOURCE_V0` in `Protocol.java` @ 0.11.0.3. The resource type is the id of
 * `org.apache.kafka.common.requests.ResourceType` (`ConfigResource::TYPE_TOPIC` = 2, `TYPE_BROKER` = 4), and the
 * name is the topic name or the **broker id as a decimal string**.
 *
 * `config_names` is a NULLABLE array, and the two empty cases mean different things:
 *
 *  - `null` (the count -1, `ff ff ff ff`) asks for **every** option of the resource - that is what
 *    `DescribeConfigsRequest.configNames()` returns as `null` and what `AdminManager.describeConfigs` turns into
 *    "no filter at all";
 *  - an **empty** array asks for no option, and the resource comes back with the error code 0 and an empty entry
 *    list.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v3)"
 */
class DescribeConfigsRequestResource implements BinarySchemaInterface
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
     * Names of the options to describe, or null for every option of the resource
     *
     * @var list<string>|null
     */
    public ?array $configNames;

    /**
     * @param list<string>|null $configNames Options to describe, null for all of them
     */
    public function __construct(int $resourceType, string $resourceName, ?array $configNames = null)
    {
        $this->resourceType = $resourceType;
        $this->resourceName = $resourceName;
        $this->configNames  = $configNames === null ? null : array_values($configNames);
    }

    /**
     * Builds the wire entry of a resource the caller described with a {@see ConfigResource}
     *
     * @param list<string>|null $configNames Options to describe, null for every option of the resource
     */
    public static function fromConfigResource(ConfigResource $resource, ?array $configNames = null): self
    {
        return new self($resource->type, $resource->name, $configNames);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'resourceType' => BinarySchema::TYPE_INT8,
            'resourceName' => BinarySchema::TYPE_STRING,
            'configNames'  => [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true],
        ];
    }
}
