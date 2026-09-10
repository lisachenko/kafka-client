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
 * Supplied principalType is not supported.
 *
 * Error code 67, Kafka 1.1 (CreateDelegationToken, KIP-48): a renewer of the request has a principal type other than `User`, the only one a 1.1 broker knows.
 */
class InvalidPrincipalTypeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_PRINCIPAL_TYPE, $previous);
    }
}
