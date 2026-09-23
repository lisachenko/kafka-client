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
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * GetTelemetrySubscriptions, version 0: what the broker wants a client to measure (ApiKey 71, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   GetTelemetrySubscriptions Request (Version: 0) => client_instance_id
 *     client_instance_id => UUID   (16 raw bytes; the zero uuid on the FIRST request)
 * </pre>
 *
 * KIP-714 - *"Client metrics and observability"* - lets a broker collect the metrics of its **clients**: an
 * operator writes a `client-metrics` configuration resource (a metric prefix, a push interval and a pattern of
 * client ids), every client asks this api what it should send, and pushes it with {@see PushTelemetryRequest}
 * (key 72). This request is the first half of that handshake and the whole of its body is the id of the client
 * *instance*: a client that has none yet sends the **zero** uuid ({@see Uuid::ZERO}, `"must be set to 0 on the
 * first request"`) and the broker answers one it generated.
 *
 * **This client never sends it.** The classes of the three client-metrics apis exist so that the frames of the
 * protocol are documented and replayable, and there is no telemetry emitter behind them: a PHP process that lives
 * for one request has nothing to report over a 300-second push interval. The api is also invisible in an
 * ApiVersions answer while the broker has no receiver plugin configured - `ApiVersionManager` @ 3.9.2 filters the
 * rows of 71 and 72 unless `ClientMetricsManager.isTelemetryReceiverConfigured()` - although it is answered all
 * the same, which is what the vectors of this api were captured with.
 *
 * @see docs/protocol/3.9.md, section "GetTelemetrySubscriptions API (key 71, v0)"
 */
class GetTelemetrySubscriptionsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * @param string $clientInstanceId Id of this client instance as 16 raw bytes, {@see Uuid::ZERO} on the first
     *        request of a process
     * @param string $clientId         A user specified identifier for the client
     * @param int    $correlationId    A value the broker passes back unmodified
     */
    public function __construct(
        protected readonly string $clientInstanceId = Uuid::ZERO,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'clientInstanceId' => BinarySchema::TYPE_UUID,
        ];
    }

    /**
     * Returns the id of the client instance this request asks for, as 16 raw bytes
     */
    public function getClientInstanceId(): string
    {
        return $this->clientInstanceId;
    }
}
