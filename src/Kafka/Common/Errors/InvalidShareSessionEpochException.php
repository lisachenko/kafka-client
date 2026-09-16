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
 * The share session epoch is invalid.
 *
 * Error code 123, Kafka 3.9: KIP-932 share groups: the share session epoch of a ShareFetch (78) or ShareAcknowledge (79) is not the one the broker expects. A `RetriableException` in the Java client, so retriable here.
 */
class InvalidShareSessionEpochException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_SHARE_SESSION_EPOCH, $previous);
    }
}
