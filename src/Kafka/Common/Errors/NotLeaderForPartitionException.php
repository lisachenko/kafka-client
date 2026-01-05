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
 * This server is not the leader for that topic-partition.
 */
class NotLeaderForPartitionException extends \RuntimeException implements KafkaException, RetriableException
{
    public function __construct($message, ?Exception $previous = null) {}
}
