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
 * The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.
 *
 * Error code 110, Kafka 3.5: The new consumer group protocol of KIP-848 (ConsumerGroupHeartbeat, 68): the member epoch of the request is behind the one the coordinator holds because the member was fenced; it rejoins with epoch 0.
 */
class FencedMemberEpochException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::FENCED_MEMBER_EPOCH, $previous);
    }
}
