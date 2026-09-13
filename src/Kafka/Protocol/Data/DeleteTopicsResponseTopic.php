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
 * **Kafka 2.7 added the `error_message` with the version 5** (KIP-599), which a 2.8.2 broker leaves `null` in
 * every answer it sends - the field is filled by the KRaft controller of the later lines. **Kafka 2.8 added the
 * `topic_id` with the version 6** (KIP-516) and made the name **nullable** with it: an answer names the topic by
 * the id the controller gave it next to the name the request used, and the name is null for a topic that was
 * deleted by its id and could not be resolved to one. {@see DeleteTopicsResponseTopicV5} is the entry of the
 * version 5 and {@see DeleteTopicsResponseTopicV0} the one of every version below it.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
class DeleteTopicsResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the entry this class stands for, the one Kafka 2.8 gave a topic id
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
