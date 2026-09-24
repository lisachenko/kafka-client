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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * ListClientMetricsResources, version 1: the config resources of the cluster (ApiKey 74, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   ListClientMetricsResources Request (Version: 0 to 1) => [resource_types]
 *     resource_types => INT8   -- since version 1 (a compact array of int8)
 * </pre>
 *
 * **Version 0 has no body at all.** `ListClientMetricsResourcesRequest.json` @ 3.7.2 declares `"fields": []`, so the
 * whole frame is the request header v2 and the tag buffer that every flexible structure ends in - the question is
 * the api key itself, and it answers the **names** of the `client-metrics` configuration resources of the cluster,
 * i.e. the subscriptions an operator wrote with `kafka-configs.sh --entity-type client-metrics`.
 * {@see ListClientMetricsResourcesRequestV0} is that frame.
 *
 * **Version 1 (Kafka 4.1, KIP-1142) turned the api into `ListConfigResources`.** The specification was renamed to
 * `ListConfigResourcesRequest.json` @ 4.1.0 - "Version 0 is used as ListClientMetricsResourcesRequest which only
 * lists client metrics resources. Version 1 adds ResourceTypes field (KIP-1142). If there is no specified
 * ResourceTypes, it should return all configuration resources." - and `Admin.listConfigResources()` of the Java
 * client @ 4.1.0 sends it; `Admin.listClientMetricsResources()` sends the same version with the one type
 * `CLIENT_METRICS`. The published name of this class stays (rule 2 of `CLAUDE.md`). The types are the bytes of
 * `ConfigResource.Type` @ 4.1.0, the constants `RESOURCE_TYPE_*` below - not the ACL resource types of
 * {@see \Protocol\Kafka\Admin\ConfigResource}, whose group is the 3 of `ResourceType` where a config resource of a
 * group is the **32**.
 *
 * @see docs/protocol/4.3.md, sections "ListClientMetricsResources API (key 74, v0 and v1)" and "The config resources
 *      of KIP-1142 (v1)"
 */
class ListClientMetricsResourcesRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::LIST_CLIENT_METRICS_RESOURCES;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * A topic, `ConfigResource.Type.TOPIC` @ 4.1.0
     */
    public const int RESOURCE_TYPE_TOPIC = 2;

    /**
     * A broker, `ConfigResource.Type.BROKER` @ 4.1.0
     */
    public const int RESOURCE_TYPE_BROKER = 4;

    /**
     * The loggers of a broker, `ConfigResource.Type.BROKER_LOGGER` @ 4.1.0
     */
    public const int RESOURCE_TYPE_BROKER_LOGGER = 8;

    /**
     * A client-metrics subscription of KIP-714, `ConfigResource.Type.CLIENT_METRICS` @ 4.1.0, and the `"default": 16`
     * of the `resource_type` of an answer
     */
    public const int RESOURCE_TYPE_CLIENT_METRICS = 16;

    /**
     * A group, `ConfigResource.Type.GROUP` @ 4.1.0 (the group configurations of KIP-848 and KIP-932)
     */
    public const int RESOURCE_TYPE_GROUP = 32;

    /**
     * @param string    $clientId      A user specified identifier for the client
     * @param int       $correlationId A value the broker passes back unmodified
     * @param list<int> $resourceTypes Types of the resources to list, the `RESOURCE_TYPE_*` bytes; empty for the
     *        default types of the broker (KIP-1142, version 1)
     */
    public function __construct(
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Types of the config resources to list, empty for the default types of the broker.
         *
         * @var list<int>
         *
         * @since Version 1 of protocol (Kafka 4.1, KIP-1142)
         */
        protected readonly array $resourceTypes = []
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     *
     * The header alone at version 0: the specification gives that version no field of its own.
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        if (static::VERSION < 1) {
            return $header;
        }

        return $header + [
            'resourceTypes' => [BinarySchema::TYPE_INT8],
        ];
    }

    /**
     * Returns the types of the config resources this request lists, empty for the default types (KIP-1142)
     *
     * @return list<int>
     */
    public function getResourceTypes(): array
    {
        return $this->resourceTypes;
    }
}
