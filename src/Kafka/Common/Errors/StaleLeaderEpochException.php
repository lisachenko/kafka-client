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
 * The leader epoch of this request is stale; another leader has been elected in the meantime.
 *
 * This class has no counterpart on the later protocol lines: error code 13 is StaleLeaderEpochCode in
 * kafka/common/ErrorMapping.scala @ 0.8.2.2, while `main` maps 13 to NetworkException. The merge of 0.8.x into 0.9.x
 * therefore has to remap code 13 consciously instead of taking this class along.
 */
class StaleLeaderEpochException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::STALE_LEADER_EPOCH, $previous);
    }
}
