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

/**
 * ListClientMetricsResources response object, version 0 (key 74, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   ListClientMetricsResources Response (Version: 0) => throttle_time_ms error_code [client_metrics_resources]
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     client_metrics_resources => name
 *       name => COMPACT_STRING
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
 * @see docs/protocol/4.3.md, section "ListClientMetricsResources API (key 74, v0)"
 */
class ListClientMetricsResourcesResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

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
     * Client-metrics subscriptions of the cluster, indexed by their name
     *
     * @var array<string, ClientMetricsResource>
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
            'clientMetricsResources' => ['name' => ClientMetricsResource::class],
        ];
    }
}
