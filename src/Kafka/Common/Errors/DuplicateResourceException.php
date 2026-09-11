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
 * A request illegally referred to the same resource twice.
 *
 * Error code 92, Kafka 2.7 (KIP-554): an AlterUserScramCredentials (51) or a DescribeUserScramCredentials (50) named
 * the same user and mechanism more than once.
 */
class DuplicateResourceException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::DUPLICATE_RESOURCE, $previous);
    }
}
