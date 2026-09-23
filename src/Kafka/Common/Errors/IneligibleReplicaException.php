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
 * The new ISR contains at least one ineligible replica.
 *
 * Error code 107, Kafka 3.3: AlterPartition (56, the AlterIsr of Kafka 2.7) asked the controller to add a fenced or shutting-down replica to the ISR; a broker-to-controller code.
 */
class IneligibleReplicaException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INELIGIBLE_REPLICA, $previous);
    }
}
