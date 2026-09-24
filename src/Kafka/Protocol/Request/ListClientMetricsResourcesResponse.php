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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ClientMetricsResource;
use Protocol\Kafka\Protocol\Data\ClientMetricsResourceV0;

/**
 * ListClientMetricsResources response object, version 1 (key 74, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   ListClientMetricsResources Response (Version: 0 to 1) => throttle_time_ms error_code [client_metrics_resources]
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     client_metrics_resources => name resource_type
 *       name          => COMPACT_STRING
 *       resource_type => INT8            -- since version 1, "default": 16 (CLIENT_METRICS)
 * </pre>
 *
 * The names of the `client-metrics` configuration resources of the cluster - the subscriptions of KIP-714 - and
 * an **empty array** on a cluster where no operator has written one, which is the answer of a node that collects
 * no client metrics at all. The array is keyed by the name here, because the name is all a resource has.
 *
 * The one refusal of the api is the **31** (`ClusterAuthorizationFailed`) of a principal without `DESCRIBE_CONFIGS`
 * on the cluster: `KafkaApis.handleListClientMetricsResources` @ 3.9.2 checks that operation before it asks its
 * `ClientMetricsManager` anything.
 *
 * **Version 1 (Kafka 4.1, KIP-1142) is the answer of `ListConfigResources`**: "Version 1 adds ResourceType to
 * ConfigResources (KIP-1142)" (`ListConfigResourcesResponse.json` @ 4.1.0). Every entry names a config resource of
 * one of the types the request asked for, with the byte of its type - a topic `t` and a group `t` are two entries
 * of the same name - so the entries of this version are a **list**, in the order of the answer, where those of
 * {@see ListClientMetricsResourcesResponseV0} are keyed by the name that is all a v0 entry has.
 *
 * @see docs/protocol/4.3.md, sections "ListClientMetricsResources API (key 74, v0 and v1)" and "The config
 *      resources of KIP-1142 (v1)"
 */
class ListClientMetricsResourcesResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the request, 0 when the list could be answered
     */
    public int $errorCode;

    /**
     * Config resources of the answer: at version 0 the client-metrics subscriptions indexed by their name, at
     * version 1 a list of the resources of every type the request asked for
     *
     * @var array<array-key, ClientMetricsResource>
     */
    public array $clientMetricsResources = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs'         => BinarySchema::TYPE_INT32,
            'errorCode'              => BinarySchema::TYPE_INT16,
            'clientMetricsResources' => static::VERSION >= 1
                ? [ClientMetricsResource::class]
                : ['name' => ClientMetricsResourceV0::class],
        ];
    }
}
