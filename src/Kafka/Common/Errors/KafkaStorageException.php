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
 * Disk error when trying to access log file on the disk.
 *
 * Error code 56, Kafka 1.0 (KIP-112/113, JBOD): the broker could not read or write the log of the partition because its log directory went offline. `KafkaStorageException` extends `InvalidMetadataException` in the Java client, so the code is retriable: the partition moves to another replica and a metadata refresh finds the new leader. Produce v4 and Fetch v6 exist only to tell the broker that the client understands this code; a lower version is answered with 6 (NotLeaderForPartition) instead.
 */
class KafkaStorageException extends KafkaException implements RetriableException, ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::KAFKA_STORAGE_ERROR, $previous);
    }
}
