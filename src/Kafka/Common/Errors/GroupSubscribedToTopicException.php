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
 * Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.
 *
 * Error code 86, Kafka 2.4 (KIP-496, OffsetDelete): an OffsetDelete (47) named a topic that a live member of the group
 * is subscribed to; the offsets of a topic can only be deleted while nobody consumes it.
 */
class GroupSubscribedToTopicException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::GROUP_SUBSCRIBED_TO_TOPIC, $previous);
    }
}
