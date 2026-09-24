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
 * Client metadata is stale. The client should rebootstrap to obtain new metadata.
 *
 * Error code 129, Kafka 4.0: KIP-1102: the top-level error code of a Metadata (3) v13 answer telling the client to drop its view of the cluster and start again from the bootstrap servers.
 */
class RebootstrapRequiredException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::REBOOTSTRAP_REQUIRED, $previous);
    }
}
