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

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * GetTelemetrySubscriptions response object, version 0 (key 71, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   GetTelemetrySubscriptions Response (Version: 0) => throttle_time_ms error_code client_instance_id
 *                                                      subscription_id [accepted_compression_types]
 *                                                      push_interval_ms telemetry_max_bytes delta_temporality
 *                                                      [requested_metrics]
 *     throttle_time_ms          => INT32
 *     error_code                => INT16
 *     client_instance_id        => UUID
 *     subscription_id           => INT32
 *     accepted_compression_types => compact array of INT8
 *     push_interval_ms          => INT32
 *     telemetry_max_bytes       => INT32
 *     delta_temporality         => BOOLEAN
 *     requested_metrics         => compact array of COMPACT_STRING
 * </pre>
 *
 * The subscription the broker hands out: which metrics it wants (a list of **prefixes**, where an empty array is
 * "nothing at all" and a single empty string is "everything"), how often, how many bytes of
 * OpenTelemetry `MetricsData` it accepts in one push and which codecs it takes for them.
 *
 * **`client_instance_id` is only filled when the request carried the zero uuid** - `"Assigned client instance id
 * if ClientInstanceId was 0 in the request, else 0"` - so a client reads its own id out of the **first** answer
 * and repeats it in every request afterwards. The **`subscription_id`** is the CRC-32C of the subscription the
 * broker computed for this instance, xor the hash of the instance id (`ClientMetricsManager` @ 3.9.2), and it is
 * what a {@see PushTelemetryRequest} has to echo: a push that carries another one is refused **117**
 * (`UnknownSubscriptionId`), which is how a client learns that the subscription has changed under it.
 *
 * `delta_temporality` says whether a counter is pushed as the difference to the last push or as its running
 * total; a 3.9.2 node always answers **true**.
 *
 * @see docs/protocol/4.3.md, section "GetTelemetrySubscriptions API (key 71, v0)"
 */
class GetTelemetrySubscriptionsResponse extends AbstractResponse
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
     * Error of the request, 0 when a subscription could be answered
     */
    public int $errorCode;

    /**
     * Id the broker assigned to this client instance, {@see Uuid::ZERO} when the request already carried one
     */
    public string $clientInstanceId = Uuid::ZERO;

    /**
     * Identifier of the subscription set this instance is on, 0 while the cluster has no `client-metrics` resource
     */
    public int $subscriptionId = 0;

    /**
     * Codecs the broker accepts for a pushed metrics blob, as the ids of `CompressionType`
     *
     * @var list<int>
     */
    public array $acceptedCompressionTypes = [];

    /**
     * How long the client waits between two pushes, in milliseconds
     */
    public int $pushIntervalMs = 0;

    /**
     * Largest metrics blob the broker accepts in one {@see PushTelemetryRequest}
     */
    public int $telemetryMaxBytes = 0;

    /**
     * Whether a monotonic metric is pushed as a delta rather than as its cumulative value
     */
    public bool $deltaTemporality = false;

    /**
     * Metric prefixes the broker asks for; an empty list is "none", a single empty string is "all"
     *
     * @var list<string>
     */
    public array $requestedMetrics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs'           => BinarySchema::TYPE_INT32,
            'errorCode'                => BinarySchema::TYPE_INT16,
            'clientInstanceId'         => BinarySchema::TYPE_UUID,
            'subscriptionId'           => BinarySchema::TYPE_INT32,
            'acceptedCompressionTypes' => [BinarySchema::TYPE_INT8],
            'pushIntervalMs'           => BinarySchema::TYPE_INT32,
            'telemetryMaxBytes'        => BinarySchema::TYPE_INT32,
            'deltaTemporality'         => BinarySchema::TYPE_BOOLEAN,
            'requestedMetrics'         => [BinarySchema::TYPE_STRING],
        ];
    }
}
