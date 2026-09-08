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
 * Error code 13 was StaleLeaderEpochCode in kafka/common/ErrorMapping.scala @ 0.8.2.2 and was never produced by a
 * 0.8 broker; from Kafka 0.9 onwards it is NETWORK_EXCEPTION (clients/.../common/protocol/Errors.java @ 0.9.0.1),
 * which is what this class carries. The Java class extends InvalidMetadataException, hence the retriable marker:
 * a dropped connection is cured by refreshing the metadata and sending the request again. The socket layer of this
 * client raises the very same exception for a connection that dies locally.
 */
class NetworkException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::NETWORK_EXCEPTION, $previous);
    }
}
