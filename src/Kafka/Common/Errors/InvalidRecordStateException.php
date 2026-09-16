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
 * The record state is invalid. The acknowledgement of delivery could not be completed.
 *
 * Error code 121, Kafka 3.9: KIP-932 share groups: a ShareAcknowledge (79) acknowledged a record whose delivery state does not allow it. Share groups are early access in 3.9 and not implemented on this line.
 */
class InvalidRecordStateException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_RECORD_STATE, $previous);
    }
}
