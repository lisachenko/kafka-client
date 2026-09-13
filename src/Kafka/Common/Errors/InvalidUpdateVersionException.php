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
 * The given update version was invalid.
 *
 * Error code 95, Kafka 2.7 (KIP-584, UpdateFeatures): an UpdateFeatures (57) asked for a finalized feature level the
 * brokers of the cluster cannot serve, or a downgrade without `allow_downgrade`.
 */
class InvalidUpdateVersionException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_UPDATE_VERSION, $previous);
    }
}
