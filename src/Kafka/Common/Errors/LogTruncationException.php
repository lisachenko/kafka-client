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
 * The log of a partition was truncated below the position of this consumer, and no reset policy allows a jump.
 *
 * **This is a client-side exception: there is no error code for it.** It is the conclusion of the position
 * validation of **KIP-320** (Kafka 2.1), which the consumer performs after it has seen a *new* leader epoch for a
 * partition: it asks the new leader with an OffsetForLeaderEpoch v2 where the epoch of its own position ended, and
 * a leader that answers an `end_offset` **below** that position has told it, in so many words, that the records it
 * was about to read never made it into this leadership - an unclean leader election, or a leader that was elected
 * from a replica that had not caught up.
 *
 * Before KIP-320 a consumer read past that silently: it kept its offset, the new leader served whatever it had at
 * that offset, and the consumer saw *different records* under the offsets it had already reported. That is the
 * "log divergence" the KIP is named after.
 *
 * What happens next is `auto.offset.reset`: with `earliest` or `latest` the consumer resets the position and goes
 * on, and only with **`none`** does this exception reach the caller - the Java client does exactly the same, with
 * the class of the same name. The context carries `topic`, `partition`, the `offset` the consumer stood at and the
 * `truncationOffset` the leader answered, so that an application can report the gap it lost.
 *
 * @see \Protocol\Kafka\Consumer\KafkaConsumer::validatePositionsIfNeeded()
 * @see docs/protocol/2.8.md, section "KIP-320 in the consumer: leader epochs and truncation detection"
 */
class LogTruncationException extends KafkaException implements ClientExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNKNOWN, $previous);
    }
}
