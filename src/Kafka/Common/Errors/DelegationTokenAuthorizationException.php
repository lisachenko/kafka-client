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
 * Delegation Token authorization failed.
 *
 * Error code 65, Kafka 1.1 (the delegation token apis 38-41, KIP-48): the authorizer of the broker refused the token operation to the principal. `DelegationTokenAuthorizationException` extends `AuthorizationException` in the Java client; like 29-31 and 53 it needs a broker with an `authorizer.class.name` to be observed.
 */
class DelegationTokenAuthorizationException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DELEGATION_TOKEN_AUTHORIZATION_FAILED, $previous);
    }
}
