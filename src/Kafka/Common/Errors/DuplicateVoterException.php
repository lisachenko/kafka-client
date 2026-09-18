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
 * The voter is already part of the set of voters.
 *
 * Error code 126, Kafka 3.9: KIP-853: AddRaftVoter (80) named a voter the quorum already has.
 */
class DuplicateVoterException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DUPLICATE_VOTER, $previous);
    }
}
