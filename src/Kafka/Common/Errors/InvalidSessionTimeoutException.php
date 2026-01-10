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
 * The session timeout is not within the range allowed by the broker
 *
 * as configured by group.min.session.timeout.ms and group.max.session.timeout.ms
 */
class InvalidSessionTimeoutException extends KafkaException
{
    public function __construct(array $context, ?Exception $previous = null) {}
}
