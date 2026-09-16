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
 * The AlterPartition request successfully updated the partition state but the leader has changed.
 *
 * Error code 108, Kafka 3.3: The controller applied an AlterPartition (56) and elected a new leader in the same step; a broker-to-controller code.
 */
class NewLeaderElectedException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::NEW_LEADER_ELECTED, $previous);
    }
}
