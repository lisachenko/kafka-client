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
 * Delegation Token is expired.
 *
 * Error code 66, Kafka 1.1 (RenewDelegationToken, ExpireDelegationToken, KIP-48): the token passed its expiry time and can not be renewed or expired any further.
 */
class DelegationTokenExpiredException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DELEGATION_TOKEN_EXPIRED, $previous);
    }
}
