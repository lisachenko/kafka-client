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
 * The group id does not exist.
 *
 * Error code 69, Kafka 1.1 (DeleteGroups, KIP-229): the coordinator has no group of that id.
 */
class GroupIdNotFoundException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::GROUP_ID_NOT_FOUND, $previous);
    }
}
