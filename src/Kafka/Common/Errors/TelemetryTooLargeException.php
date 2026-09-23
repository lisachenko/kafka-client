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

namespace Protocol\Kafka\Common\Errors;

use Exception;

/**
 * Client sent a push telemetry request larger than the maximum size the broker will accept.
 *
 * Error code 118, Kafka 3.7: KIP-714 client metrics: the metrics payload of a PushTelemetry (72) exceeds `telemetry.max.bytes` of the broker.
 */
class TelemetryTooLargeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::TELEMETRY_TOO_LARGE, $previous);
    }
}
