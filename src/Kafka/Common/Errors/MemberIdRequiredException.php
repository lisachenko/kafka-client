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
 * The group member needs to have a valid member id before actually entering a consumer group.
 *
 * Error code 79, Kafka 2.2 (KIP-394, JoinGroup v4): a JoinGroup v4 or above with an empty member id is answered with
 * this code and the member id the coordinator assigned, and the client joins again with that id; the coordinator no
 * longer adds a member it cannot identify to a rebalance.
 */
class MemberIdRequiredException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::MEMBER_ID_REQUIRED, $previous);
    }
}
