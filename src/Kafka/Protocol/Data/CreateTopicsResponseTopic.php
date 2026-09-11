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
 * The result of creating one topic, version 1 of the CreateTopics API
 *
 * <pre>
 *   CreateTopicsResponseTopic => Topic ErrorCode ErrorMessage
 *     Topic        => string
 *     ErrorCode    => int16
 *     ErrorMessage => nullable string
 * </pre>
 *
 * `TOPIC_ERROR` in `Protocol.java` @ 0.10.2.2, which "improves on TOPIC_ERROR_CODE by adding an error_message to
 * complement the error_code": version 0 of the api answers with the bare `Topic ErrorCode` of
 * {@see CreateTopicsResponseTopicV0}, version 1 adds the message that the broker logged for itself before. The
 * message is null whenever the broker has none - for a topic that was created without an error, and for the error
 * codes that `KafkaApis.handleCreateTopicsRequest` produces without an exception (41 NotController and 31
 * ClusterAuthorizationFailed).
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0, v1 and v2)"
 */
class CreateTopicsResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the CreateTopics API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * Name of the topic that was requested
     */
    public string $topic;

    /**
     * Error code of this topic, 0 when it was created
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when the broker sent none
     *
     * @since Version 1 of protocol
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'topic'     => BinarySchema::TYPE_STRING,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
        if (static::VERSION >= 1) {
            $scheme['errorMessage'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $scheme;
    }
}
