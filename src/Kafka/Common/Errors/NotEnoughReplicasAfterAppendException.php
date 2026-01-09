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
 * Messages are written to the log, but to fewer in-sync replicas than required.
 */
class NotEnoughReplicasAfterAppendException extends KafkaException implements RetriableException
{
    public function __construct($message, ?Exception $previous = null)
    {
        parent::__construct($message, self::NOT_ENOUGH_REPLICAS_AFTER_APPEND, $previous);
    }
}
