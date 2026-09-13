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
 * There is no listener on the leader broker that matches the listener on which metadata request was processed.
 *
 * Error code 72, Kafka 2.0 (KAFKA-6796, Metadata): the leader of the partition has no listener of the security
 * protocol the Metadata request arrived on, so the broker cannot name an endpoint for it. `ListenerNotFoundException`
 * extends `InvalidMetadataException` in the Java client, so the code is retriable like 5 and 6: a later metadata
 * refresh may find a leader that has the listener.
 */
class ListenerNotFoundException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::LISTENER_NOT_FOUND, $previous);
    }
}
