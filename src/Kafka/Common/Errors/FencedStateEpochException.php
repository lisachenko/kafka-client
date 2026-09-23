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
 * The share coordinator rejected the request because the share-group state epoch did not match.
 *
 * Error code 124, Kafka 3.9: KIP-932 share groups: the share-group state apis 83 to 87 carried a state epoch the share coordinator has fenced.
 */
class FencedStateEpochException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::FENCED_STATE_EPOCH, $previous);
    }
}
