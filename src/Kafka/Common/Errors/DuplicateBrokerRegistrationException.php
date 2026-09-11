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
 * This broker ID is already in use.
 *
 * Error code 101, Kafka 2.8 (KIP-631, BrokerRegistration): a BrokerRegistration (62) of the KRaft mode named a broker
 * id that is already registered; controller traffic only.
 */
class DuplicateBrokerRegistrationException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DUPLICATE_BROKER_REGISTRATION, $previous);
    }
}
