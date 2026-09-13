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
 * Leader election not needed for topic partition.
 *
 * Error code 84, Kafka 2.4 (KIP-460, ElectLeaders v1): the leader the election would produce is already the leader of
 * the partition. An `InvalidMetadataException` in the Java client, so retriable.
 */
class ElectionNotNeededException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::ELECTION_NOT_NEEDED, $previous);
    }
}
