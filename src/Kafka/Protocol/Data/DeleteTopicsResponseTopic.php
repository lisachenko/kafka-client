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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of deleting one topic, i.e. one entry of the `topic_error_codes` array
 *
 * <pre>
 *   DeleteTopicsResponseTopic => Topic ErrorCode
 *     Topic     => string
 *     ErrorCode => int16
 * </pre>
 *
 * `TOPIC_ERROR_CODE` in `Protocol.java` @ 0.10.2.2. The only version of the DeleteTopics api is 0, so there is no
 * `ErrorMessage` here - the reason of a failure is only in the log of the broker.
 *
 * A topic the broker does not know is answered with the error code 3 (UnknownTopicOrPartition), a topic that was
 * only marked for deletion because the request carried a timeout of 0 with 7 (RequestTimedOut), and every topic of
 * the request with 41 (NotController) when the broker is not the active controller.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
class DeleteTopicsResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the entry this class stands for, the one Kafka 2.7 gave an error message
     */
    public const int VERSION = 6;

    /**
     * The `topic_id` of an answer that names the topic by its name alone: 16 zero bytes
     *
     * @since Version 6 of protocol
     */
    public const string NO_TOPIC_ID = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

    /**
     * Name of the topic that was requested, `null` for a topic the request named by its id alone
     */
    public ?string $topic;

    /**
     * Id of the deleted topic as the raw 16 bytes of its UUID (KIP-516), {@see self::NO_TOPIC_ID} below version 6
     *
     * @since Version 6 of protocol
     */
    public string $topicId = self::NO_TOPIC_ID;

    /**
     * Error code of this topic, 0 when it was deleted
     */
    public int $errorCode;

    /**
     * Message of the broker for a topic it refused, `null` for one it deleted
     *
     * @since Version 5 of protocol
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            // The name became nullable with the version 6, which may name a topic by its id alone
            'topic' => static::VERSION >= 6 ? BinarySchema::TYPE_NULLABLE_STRING : BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 6) {
            $scheme['topicId'] = BinarySchema::TYPE_UUID;
        }
        $scheme['errorCode'] = BinarySchema::TYPE_INT16;
        if (static::VERSION >= 5) {
            $scheme['errorMessage'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $scheme;
    }
}
