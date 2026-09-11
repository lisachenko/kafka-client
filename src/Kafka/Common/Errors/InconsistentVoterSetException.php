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
 * Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters
 *
 * Error code 94, Kafka 2.7 (KIP-595, the raft apis): a Vote, BeginQuorumEpoch or EndQuorumEpoch request (52-54) of the
 * KRaft quorum names a voter that is not part of the quorum; controller traffic only, never served by a
 * ZooKeeper-backed broker.
 */
class InconsistentVoterSetException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INCONSISTENT_VOTER_SET, $previous);
    }
}
