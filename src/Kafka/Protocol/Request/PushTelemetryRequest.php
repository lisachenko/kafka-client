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
 * PushTelemetry, version 0: the metrics of one client instance (ApiKey 72, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   PushTelemetry Request (Version: 0) => client_instance_id subscription_id terminating compression_type metrics
 *     client_instance_id => UUID
 *     subscription_id    => INT32
 *     terminating        => BOOLEAN
 *     compression_type   => INT8    (the id of `CompressionType`, 0 = none)
 *     metrics            => COMPACT_BYTES  (OpenTelemetry `MetricsData` v1, protobuf)
 * </pre>
 *
 * The second half of the KIP-714 handshake: the client pushes what {@see GetTelemetrySubscriptionsRequest} (key
 * 71) asked it for, every `push_interval_ms`, and the broker hands the blob to its receiver plugin. The payload
 * is **opaque to this protocol** - it is an OpenTelemetry `MetricsData` message, optionally compressed with one
 * of the codecs the subscription named - so the api carries it as a plain byte field.
 *
 * `terminating` is the last push of an instance that is shutting down: a broker takes it outside the push
 * interval and refuses **42** (`InvalidRequest`) every request that follows it on the same instance.
 *
 * **This client never sends it** - see {@see GetTelemetrySubscriptionsRequest} - and the class exists for the
 * frames alone. The refusals are worth knowing all the same, because they are the codes Kafka 3.7 added for it:
 * a `subscription_id` that is not the one of the instance is **117** (`UnknownSubscriptionId`), a blob above
 * `telemetry.max.bytes` is **118** (`TelemetryTooLarge`), a codec the broker does not accept is **76**
 * (`UnsupportedCompressionType`) and a push inside the interval is **89** (`ThrottlingQuotaExceeded`).
 *
 * @see docs/protocol/3.9.md, section "PushTelemetry API (key 72, v0)"
 */
class PushTelemetryRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::PUSH_TELEMETRY;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * The `compression_type` of a metrics blob that is not compressed at all (`CompressionType.NONE.id` @ 3.9.2)
     */
    public const int COMPRESSION_NONE = 0;

    /**
     * @param string $clientInstanceId Id of this client instance as 16 raw bytes, never {@see Uuid::ZERO}
     * @param int    $subscriptionId   The id of the subscription that asked for these metrics
     * @param string $metrics          OpenTelemetry `MetricsData` v1 as protobuf bytes
     * @param bool   $terminating      Whether this is the last push of an instance that is shutting down
     * @param int    $compressionType  Codec of the blob, {@see self::COMPRESSION_NONE} for none
     * @param string $clientId         A user specified identifier for the client
     * @param int    $correlationId    A value the broker passes back unmodified
     */
    public function __construct(
        protected readonly string $clientInstanceId,
        protected readonly int $subscriptionId,
        protected readonly string $metrics = '',
        protected readonly bool $terminating = false,
        protected readonly int $compressionType = self::COMPRESSION_NONE,
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
            'subscriptionId'   => BinarySchema::TYPE_INT32,
            'terminating'      => BinarySchema::TYPE_BOOLEAN,
            'compressionType'  => BinarySchema::TYPE_INT8,
            'metrics'          => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Returns the id of the client instance these metrics belong to, as 16 raw bytes
     */
    public function getClientInstanceId(): string
    {
        return $this->clientInstanceId;
    }

    /**
     * Returns the id of the subscription this push answers
     */
    public function getSubscriptionId(): int
    {
        return $this->subscriptionId;
    }

    /**
     * Returns whether this is the last push of the instance
     */
    public function isTerminating(): bool
    {
        return $this->terminating;
    }

    /**
     * Returns the codec of the metrics blob
     */
    public function getCompressionType(): int
    {
        return $this->compressionType;
    }

    /**
     * Returns the metrics blob itself, as the raw bytes the broker passes on to its receiver plugin
     */
    public function getMetrics(): string
    {
        return $this->metrics;
    }
}
