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
 * Requested snapshot was not found
 *
 * Error code 98, Kafka 2.8 (KIP-630, FetchSnapshot): a FetchSnapshot (59) asked for a snapshot the leader of the
 * metadata log does not have; controller traffic only.
 */
class SnapshotNotFoundException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::SNAPSHOT_NOT_FOUND, $previous);
    }
}
