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
 * The group is not empty.
 *
 * Error code 68, Kafka 1.1 (DeleteGroups, KIP-229): a group can only be deleted while it has no members; a group in the `Empty` or `Dead` state can, one with a live member can not. The class name is the one of the Java client (`GroupNotEmptyException`), the constant the name of the error code.
 */
class GroupNotEmptyException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::NON_EMPTY_GROUP, $previous);
    }
}
