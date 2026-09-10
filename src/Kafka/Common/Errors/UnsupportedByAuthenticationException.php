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
 * Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.
 *
 * Error code 64, Kafka 1.1 (the delegation token apis 38-41, KIP-48): a token is issued to a SASL principal, so a connection without one - PLAINTEXT, SSL without a client certificate - can not ask for one, and a connection that authenticated with a token can not create another. The class name is the one of the Java client (`UnsupportedByAuthenticationException`), the constant the name of the error code.
 */
class UnsupportedByAuthenticationException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, $previous);
    }
}
