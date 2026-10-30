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
 * DeleteGroups could not complete; see the error message on the per-group result for details.
 *
 * Error code 134, Kafka 4.4: KIP-1331: the per-group code of a DeleteGroups (42) answer when the coordinator could not finish deleting the group (the topology description plugin of a streams group failed to drop its description); the group is kept, the v3 answer carries the cause as the `error_message` of the entry, and a retry is safe.
 */
class GroupDeletionFailedException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::GROUP_DELETION_FAILED, $previous);
    }
}
