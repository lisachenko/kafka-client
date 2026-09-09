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
 * The version of API is not supported.
 *
 * Error code 35, Kafka 0.10.0 (ApiVersions, KIP-35). The only request a 0.10 broker answers with it is an ApiVersions request of an unknown version; any other unparsable request closes the connection instead.
 */
class UnsupportedVersionException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNSUPPORTED_VERSION, $previous);
    }
}
