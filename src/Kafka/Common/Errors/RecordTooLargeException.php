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

use RuntimeException;

/**
 * A record in the log is bigger than the fetch size the consumer asked for, so it can never be received.
 *
 * A Kafka 0.9.0.1 broker cuts a fetched message set off at `MaxBytes` without guaranteeing any progress: when the
 * very first message at the requested offset is bigger than that limit, the partition comes back without an error
 * and without a single complete message, while its high water mark still shows unread data. A consumer that keeps
 * asking for the same offset would spin forever, therefore it raises this error instead - the only way out is a
 * bigger `max.partition.fetch.bytes`.
 *
 * This is a client-side error: unlike MessageTooLargeException (broker error code 10), which rejects a *produced*
 * message set that exceeds `message.max.bytes`, no error code travels over the wire for this one.
 */
class RecordTooLargeException extends RuntimeException implements ClientExceptionInterface
{
    /**
     * @param string $topic          Topic of the partition that can not make progress
     * @param int    $partition      Id of that partition
     * @param int    $fetchOffset    Offset that was requested and could not be read
     * @param int    $maxBytes       Value of `max.partition.fetch.bytes` that was used for the request
     * @param int    $logEndOffset   Offset of the next message that would be appended to that partition
     */
    public function __construct(
        public readonly string $topic,
        public readonly int $partition,
        public readonly int $fetchOffset,
        public readonly int $maxBytes,
        public readonly int $logEndOffset,
    ) {
        parent::__construct(
            sprintf(
                'The message at the offset %d of %s:%d is larger than the configured %s of %d bytes: the broker '
                . 'answered with no complete message while the log of that partition ends at %d. Increase '
                . 'max.partition.fetch.bytes above the size of that message to consume it.',
                $fetchOffset,
                $topic,
                $partition,
                'max.partition.fetch.bytes',
                $maxBytes,
                $logEndOffset
            )
        );
    }
}
