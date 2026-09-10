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
 * Delegation Token feature is not enabled.
 *
 * Error code 61, Kafka 1.1 (the delegation token apis 38-41, KIP-48): the broker has no `delegation.token.master.key`, so no token can be created, renewed, expired or described.
 */
class DelegationTokenDisabledException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DELEGATION_TOKEN_AUTH_DISABLED, $previous);
    }
}
