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
 * The requesting client does not support the compression type of given partition.
 *
 * Error code 76, Kafka 2.1 (KIP-110, zstd): a Fetch below v10 asked for a partition whose records are compressed with
 * zstd, which only a client of that version or above promises to understand; a broker does not down-convert zstd.
 */
class UnsupportedCompressionTypeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNSUPPORTED_COMPRESSION_TYPE, $previous);
    }
}
