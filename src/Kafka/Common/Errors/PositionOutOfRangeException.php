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
 * Requested position is not greater than or equal to zero, and less than the size of the snapshot.
 *
 * Error code 99, Kafka 2.8 (KIP-630, FetchSnapshot): a FetchSnapshot (59) asked for a position outside the snapshot;
 * controller traffic only.
 */
class PositionOutOfRangeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::POSITION_OUT_OF_RANGE, $previous);
    }
}
