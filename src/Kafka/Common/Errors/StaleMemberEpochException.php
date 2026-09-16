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
 * Error code 113, Kafka 3.6: KIP-848: an OffsetCommit v9 or OffsetFetch v9 carried a member epoch below the one the coordinator holds for the member.
 */
class StaleMemberEpochException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::STALE_MEMBER_EPOCH, $previous);
    }
}
