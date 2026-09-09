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
 * Configuration is invalid.
 *
 * Error code 40, Kafka 0.10.1 (CreateTopics, KIP-4). The Java client calls it InvalidConfigurationException; that name is taken on this client by the client-side {@see InvalidConfigurationException} of a bad client configuration, which is not a protocol error.
 */
class InvalidConfigException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_CONFIG, $previous);
    }
}
