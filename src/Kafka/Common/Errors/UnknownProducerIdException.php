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
 * This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question.
 *
 * Error code 59, Kafka 1.0: the broker lost the state of the producer id, for instance because every record of the producer was deleted from the log by retention or by DeleteRecords - once the last records of a producer id are gone, its metadata is removed and a later append with that id is answered with this code. `UnknownProducerIdException` extends `OutOfOrderSequenceException` in the Java client, and the 1.x producer answers it by resetting the sequence numbers of the partition when the `log_start_offset` of the Produce v5 answer shows that its records were deleted.
 */
class UnknownProducerIdException extends OutOfOrderSequenceException
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        // The code of this exception is 59, not the 45 of its parent: the grandparent constructor is called on purpose
        KafkaException::__construct($context, self::UNKNOWN_PRODUCER_ID, $previous);
    }
}
