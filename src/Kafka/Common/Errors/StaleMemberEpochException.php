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
 * The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.
 *
 * Error code 113, Kafka 3.6: KIP-848: an OffsetCommit v9 or OffsetFetch v9 carried a member epoch that is not the
 * one the coordinator holds for the member. Measured on a 3.9.2 node with OffsetCommit v9: a member of a KIP-848
 * group is answered this code per partition for an epoch below **and** above its current one, while a member of a
 * classic group is answered 22 (`IllegalGeneration`) for the same frame and may not send a version below 9 at all
 * (35, `UnsupportedVersion`).
 *
 * @see docs/protocol/3.9.md, section "The member epoch of KIP-848 (v9)"
 */
class StaleMemberEpochException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::STALE_MEMBER_EPOCH, $previous);
    }
}
