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
 * This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.
 *
 * Named InvalidMessageCode (2) in kafka/common/ErrorMapping.scala @ 0.8.2.2.
 */
class CorruptMessageException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::CORRUPT_MESSAGE, $previous);
    }
}
