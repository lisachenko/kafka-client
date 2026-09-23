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
 * The voter key doesn't match the receiving replica's key.
 *
 * Error code 125, Kafka 3.9: KIP-853 (dynamic KRaft quorums): a raft request addressed a voter by an id and directory id that is not the one of the receiving replica; a controller code.
 */
class InvalidVoterKeyException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_VOTER_KEY, $previous);
    }
}
