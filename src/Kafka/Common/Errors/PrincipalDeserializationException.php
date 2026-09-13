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
 * Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster
 * security setup.
 *
 * Error code 97, Kafka 2.8 (KIP-590, Envelope): the controller could not deserialize the principal a broker forwarded
 * in an Envelope (58); an error of the KRaft forwarding path, never seen by a client of a ZooKeeper-backed broker. The
 * Java class is `PrincipalDeserializationException`, without the `Failure`.
 */
class PrincipalDeserializationException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::PRINCIPAL_DESERIALIZATION_FAILURE, $previous);
    }
}
