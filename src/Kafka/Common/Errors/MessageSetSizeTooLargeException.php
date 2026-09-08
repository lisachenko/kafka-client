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
 * The request included a message set larger than the configured segment size on the server.
 */
class MessageSetSizeTooLargeException extends KafkaException
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::MESSAGE_SET_SIZE_TOO_LARGE, $previous);
    }
}
