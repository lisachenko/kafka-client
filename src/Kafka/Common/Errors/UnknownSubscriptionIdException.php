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
 * Client sent a push telemetry request with an invalid or outdated subscription ID.
 *
 * Error code 117, Kafka 3.7: KIP-714 client metrics: a PushTelemetry (72) carried a subscription id that is not the one GetTelemetrySubscriptions (71) last handed out.
 */
class UnknownSubscriptionIdException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNKNOWN_SUBSCRIPTION_ID, $previous);
    }
}
