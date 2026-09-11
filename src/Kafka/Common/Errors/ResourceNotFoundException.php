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
 * A request illegally referred to a resource that does not exist.
 *
 * Error code 91, Kafka 2.7 (KIP-554, the SCRAM credential apis): a DescribeUserScramCredentials (50) named a user
 * without a credential, or an AlterUserScramCredentials (51) tried to delete a credential the user does not have.
 */
class ResourceNotFoundException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::RESOURCE_NOT_FOUND, $previous);
    }
}
