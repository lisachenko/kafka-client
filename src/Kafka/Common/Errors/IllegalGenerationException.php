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
 * Specified group generation id is not valid.
 *
 * Listed as IllegalConsumerGeneration in the comments of kafka/common/ErrorMapping.scala @ 0.9.0.1.
 */
class IllegalGenerationException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::ILLEGAL_GENERATION, $previous);
    }
}
