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
 * Specified Principal is not valid Owner/Renewer.
 *
 * Error code 63, Kafka 1.1 (RenewDelegationToken, ExpireDelegationToken, KIP-48): the authenticated principal is neither the owner nor one of the renewers of the token.
 */
class DelegationTokenOwnerMismatchException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DELEGATION_TOKEN_OWNER_MISMATCH, $previous);
    }
}
