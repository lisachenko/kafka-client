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
 * The clusterId in the request does not match that found on the server
 *
 * Error code 104, Kafka 2.8 (KIP-595, the raft apis): a Fetch v12 or a raft request of the KRaft quorum carried a
 * `cluster_id` other than the one of the receiving node; controller traffic only.
 */
class InconsistentClusterIdException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INCONSISTENT_CLUSTER_ID, $previous);
    }
}
