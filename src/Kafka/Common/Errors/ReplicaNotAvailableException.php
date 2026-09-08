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
 * A requested replica is not available on the broker that answered the metadata request.
 */
class ReplicaNotAvailableException extends KafkaException
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::REPLICA_NOT_AVAILABLE, $previous);
    }
}
