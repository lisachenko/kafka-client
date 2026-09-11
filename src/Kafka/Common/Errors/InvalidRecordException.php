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
 * This record has failed the validation on broker and hence will be rejected.
 *
 * Error code 87, Kafka 2.4 (KIP-467, Produce v8): a record of the batch failed the validation of the broker (a
 * compacted topic without a key, an invalid timestamp, a corrupt record); a Produce v8 answer names the offending
 * records in `record_errors` and explains them in `error_message`.
 */
class InvalidRecordException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_RECORD, $previous);
    }
}
