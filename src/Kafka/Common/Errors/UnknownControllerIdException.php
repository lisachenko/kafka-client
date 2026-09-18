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
 * This controller ID is not known.
 *
 * Error code 116, Kafka 3.7: KIP-919: a request addressed a controller id the quorum does not have.
 */
class UnknownControllerIdException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::UNKNOWN_CONTROLLER_ID, $previous);
    }
}
