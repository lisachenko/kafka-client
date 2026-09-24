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

namespace Protocol\Kafka\Consumer;

use InvalidArgumentException;
use Stringable;

/**
 * A regular expression a consumer of the new consumer protocol subscribes with (KIP-848, Kafka 4.0)
 *
 * This is `org.apache.kafka.clients.consumer.SubscriptionPattern` @ 4.0.0: a regex in the **RE2/J** syntax of
 * `com.google.re2j.Pattern`, which the client does not evaluate at all. It travels as the `subscribed_topic_regex` of
 * a ConsumerGroupHeartbeat **v1** to the group coordinator, which matches it against the topics of the cluster -
 * the ones that exist now and the ones created later - and answers the partitions of the matching topics as the
 * assignment of the member. A regex the coordinator cannot compile is the **128** `InvalidRegularExpression`
 * (`Utils.throwIfRegularExpressionIsInvalid` @ 4.0.0).
 *
 * RE2 is not PCRE: it has no backreferences and no lookaround, and a pattern matches a topic name as a whole, so
 * `orders-.*` names every topic that starts with `orders-`. This is the pattern of
 * {@see KafkaConsumer::subscribeByPattern()}, which requires `group.protocol=consumer`; the classic protocol has no
 * field for it.
 *
 * @see docs/protocol/4.3.md, section "The regex subscription (v1, KIP-848)"
 */
final class SubscriptionPattern implements Stringable
{
    /**
     * @param string $pattern Regular expression in the RE2/J syntax, never empty
     *
     * @throws InvalidArgumentException If the pattern is empty
     */
    public function __construct(private readonly string $pattern)
    {
        if ($pattern === '') {
            throw new InvalidArgumentException('Topic pattern to subscribe to cannot be empty');
        }
    }

    /**
     * Returns the regular expression, as the coordinator receives it
     */
    public function pattern(): string
    {
        return $this->pattern;
    }

    public function __toString(): string
    {
        return $this->pattern;
    }
}
