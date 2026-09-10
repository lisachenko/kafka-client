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
 * The user-specified log directory is not found in the broker config.
 *
 * Error code 57, Kafka 1.0 (AlterReplicaLogDirs, KIP-113): the log directory named in the request is not one of the `log.dirs` entries of the broker.
 */
class LogDirNotFoundException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::LOG_DIR_NOT_FOUND, $previous);
    }
}
