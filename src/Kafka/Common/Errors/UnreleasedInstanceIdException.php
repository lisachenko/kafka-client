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
 * The instance ID is still used by another member in the consumer group. That member must leave first.
 *
 * Error code 111, Kafka 3.5: KIP-848: a ConsumerGroupHeartbeat (68) joined with a `group.instance.id` that another live member of the group still holds.
 */
class UnreleasedInstanceIdException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNRELEASED_INSTANCE_ID, $previous);
    }
}
