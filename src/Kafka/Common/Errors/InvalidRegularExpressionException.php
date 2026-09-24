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
 * The regular expression is not valid.
 *
 * Error code 128, Kafka 4.0: KIP-848: the `subscribed_topic_regex` of a ConsumerGroupHeartbeat (68) is not a valid RE2/J regular expression. The Java client calls the class `InvalidRegularExpression`, without the suffix every exception class of this package carries.
 */
class InvalidRegularExpressionException extends KafkaException implements ServerExceptionInterface
{
    public function __construct(array $context = [], ?Exception $previous = null)
    {
        parent::__construct($context, self::INVALID_REGULAR_EXPRESSION, $previous);
    }
}
