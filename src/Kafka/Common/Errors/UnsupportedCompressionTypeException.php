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
 *
 * **This class doubles as the client-side "this build can not speak zstd" error.** There is no pure-PHP zstd in
 * this package, so {@see \Protocol\Kafka\Common\Record\CompressionCodec} compresses and decompresses the codec
 * through `ext-zstd` and throws this very exception when the extension is not loaded - the same condition the
 * broker reports with the code 76, seen from the other side: *this client does not support the compression type of
 * that partition*. The context of such an instance carries a human-readable `error` and the `codec`, and no topic
 * or partition, which is how the two cases are told apart.
 */
class UnsupportedCompressionTypeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNSUPPORTED_COMPRESSION_TYPE, $previous);
    }
}
