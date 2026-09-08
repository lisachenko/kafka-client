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
 * The server disconnected before a response was received.
 *
 * On the 0.8 protocol line this is a purely client-side condition: error code 13 is StaleLeaderEpochCode in
 * kafka/common/ErrorMapping.scala @ 0.8.2.2, and it only became NetworkException in a later protocol line. The class
 * is therefore never produced by KafkaException::fromCode() here, it is raised by the socket layer. When 0.8.x is
 * merged into 0.9.x, this class regains its NETWORK_EXCEPTION (13) code and the ServerExceptionInterface marker.
 */
class NetworkException extends KafkaException implements RetriableException, ClientExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNKNOWN, $previous);
    }
}
