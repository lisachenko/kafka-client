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
 * The consumer group has reached its max size.
 *
 * Error code 81, Kafka 2.2 (KIP-389): a JoinGroup would take the group above `group.max.size` of the broker; the
 * member is not admitted.
 */
class GroupMaxSizeReachedException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::GROUP_MAX_SIZE_REACHED, $previous);
    }
}
