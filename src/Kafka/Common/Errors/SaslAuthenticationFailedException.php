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
 * SASL Authentication failed.
 *
 * Error code 58, Kafka 1.0 (SaslAuthenticate, KIP-152): the broker refused the credentials of the token exchange and, unlike a 0.11 broker, says so with an error code and a message instead of closing the connection. The Java client raises `SaslAuthenticationException` for it; that name belongs to the client-side {@see SaslAuthenticationException} of this package, which the socket layer throws to the application (it is deliberately not a {@see KafkaException}, so that the retry loops let it through) with this exception as its cause.
 */
class SaslAuthenticationFailedException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::SASL_AUTHENTICATION_FAILED, $previous);
    }
}
