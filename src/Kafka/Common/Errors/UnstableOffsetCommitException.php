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
 * There are unstable offsets that need to be cleared.
 *
 * Error code 88, Kafka 2.5 (KIP-447, OffsetFetch v7): an OffsetFetch v7 with `require_stable = true` asked for offsets
 * that a transaction is still committing; the consumer retries. Retriable in the Java client.
 */
class UnstableOffsetCommitException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNSTABLE_OFFSET_COMMIT, $previous);
    }
}
