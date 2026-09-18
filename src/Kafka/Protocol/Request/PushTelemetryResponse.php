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

/**
 * PushTelemetry response object, version 0 (key 72, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   PushTelemetry Response (Version: 0) => throttle_time_ms error_code
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 * </pre>
 *
 * The **smallest answer of this protocol**: a throttle time, an error code and the two tag buffers of a flexible
 * frame - eleven bytes behind the size field. The broker says nothing about what it did with the metrics, only
 * whether it took them; everything else a client needs is in the subscription it already has.
 *
 * @see docs/protocol/3.9.md, section "PushTelemetry API (key 72, v0)"
 */
class PushTelemetryResponse extends AbstractResponse
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
     * Error of the request, 0 when the broker accepted the metrics
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
        ];
    }
}
