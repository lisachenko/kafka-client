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
 * One config resource of a ListClientMetricsResources answer of version 1 (key 74, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   ClientMetricsResource => Name ResourceType
 *     Name         => COMPACT_STRING
 *     ResourceType => INT8             -- since version 1 (Kafka 4.1, KIP-1142), "default": 16
 * </pre>
 *
 * A name and nothing else - `ListClientMetricsResourcesResponse.json` @ 3.7.2 gives the structure one field, and
 * not even an `about` for it. The name is the entity name of a `client-metrics` configuration resource, i.e. what
 * `kafka-configs.sh --entity-type client-metrics --entity-name <name>` wrote; its contents are read with
 * DescribeConfigs on the resource type `CLIENT_METRICS`.
 *
 * **Version 1 (Kafka 4.1, KIP-1142) appended the type**: the api became `ListConfigResources`, and an entry is a
 * config resource of any type - `ListConfigResourcesResponseData.ConfigResource` @ 4.1.0, whose `ResourceName` is
 * this `name` and whose `ResourceType` is the byte of `ConfigResource.Type` (the `RESOURCE_TYPE_*` constants of
 * {@see \Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesRequest}). {@see ClientMetricsResourceV0} is the
 * entry of the version 0, whose type is always the `CLIENT_METRICS` of the default.
 *
 * @see docs/protocol/4.3.md, sections "ListClientMetricsResources API (key 74, v0 and v1)" and "The config
 *      resources of KIP-1142 (v1)"
 */
class ClientMetricsResource implements BinarySchemaInterface
{
    /**
     * Version of the ListClientMetricsResources API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * Name of the configuration resource: the subscription, topic, group or broker id
     */
    public string $name;

    /**
     * Type of the configuration resource, a byte of `ConfigResource.Type`; 16 (`CLIENT_METRICS`) below version 1
     *
     * @since Version 1 of protocol (Kafka 4.1, KIP-1142)
     */
    public int $resourceType = 16;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = ['name' => BinarySchema::TYPE_STRING];
        if (static::VERSION >= 1) {
            $scheme['resourceType'] = BinarySchema::TYPE_INT8;
        }

        return $scheme;
    }
}
