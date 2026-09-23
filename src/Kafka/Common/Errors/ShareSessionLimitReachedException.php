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
 * The limit of share sessions has been reached.
 *
 * Error code 133, Kafka 4.1: KIP-932 share groups: a ShareFetch (78) opening a new share session found the broker at its share-session limit. A `RetriableException` in the Java client, so retriable here.
 */
class ShareSessionLimitReachedException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::SHARE_SESSION_LIMIT_REACHED, $previous);
    }
}
