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
 * The requested offset is moved to tiered storage.
 *
 * Error code 109, Kafka 3.5: A Fetch of an offset below the local log start offset of a topic with remote storage (KIP-405 tiered storage): the data is in the remote tier and the fetch has to be served from it. The container of this line has no remote storage, so the code is implemented from `Errors.java` alone.
 */
class OffsetMovedToTieredStorageException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::OFFSET_MOVED_TO_TIERED_STORAGE, $previous);
    }
}
