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
 * The requested fetch size is invalid.
 *
 * Code 4 is InvalidFetchSizeCode in kafka/common/ErrorMapping.scala @ 0.10.2.2, which the Scala Fetch path still
 * produces, while clients/.../common/protocol/Errors.java of the same tag leaves the code free with a
 * "TODO: errorCode 4 for InvalidFetchSize". The class therefore stays, as it does on `main`.
 */
class InvalidFetchSizeException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_FETCH_SIZE, $previous);
    }
}
