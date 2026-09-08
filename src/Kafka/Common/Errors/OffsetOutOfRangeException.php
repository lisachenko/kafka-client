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
 * The requested offset is not within the range of offsets maintained by the server for the given topic/partition.
 */
class OffsetOutOfRangeException extends KafkaException
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::OFFSET_OUT_OF_RANGE, $previous);
    }
}
