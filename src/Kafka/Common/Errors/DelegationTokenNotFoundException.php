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
 * Delegation Token is not found on server.
 *
 * Error code 62, Kafka 1.1 (RenewDelegationToken, ExpireDelegationToken, KIP-48): the HMAC of the request names no token the broker knows.
 */
class DelegationTokenNotFoundException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DELEGATION_TOKEN_NOT_FOUND, $previous);
    }
}
