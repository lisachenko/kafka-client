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
 * The request included a message larger than the max message size the server will accept.
 */
class MessageTooLargeException extends \RuntimeException implements KafkaException
{
    public function __construct($message, ?Exception $previous = null)
    {
        parent::__construct($message, self::MESSAGE_TOO_LARGE, $previous);
    }
}
