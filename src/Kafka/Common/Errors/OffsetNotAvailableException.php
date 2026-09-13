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
 * The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be
 * monotonically increasing.
 *
 * Error code 78, Kafka 2.2 (KIP-207, ListOffsets v5): a ListOffsets v5 asked a leader that has just been elected and
 * whose high watermark has not caught up yet; the answer would not be monotonic, so the broker asks the client to try
 * again. Retriable in the Java client.
 */
class OffsetNotAvailableException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::OFFSET_NOT_AVAILABLE, $previous);
    }
}
