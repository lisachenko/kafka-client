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

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of one topic of a DeleteShareGroupOffsets request (ApiKey 92, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DeleteShareGroupOffsetsResponseTopic => TopicName TopicId ErrorCode ErrorMessage TAG_BUFFER
 *     TopicName    => COMPACT_STRING
 *     TopicId      => UUID
 *     ErrorCode    => INT16
 *     ErrorMessage => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "DeleteShareGroupOffsets API (key 92, v0)"
 */
final class DeleteShareGroupOffsetsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $topicName;

    /**
     * Id of the topic, {@see Uuid::ZERO} for a topic the node does not have
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Error of this topic, 0 when its state was deleted
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName'    => BinarySchema::TYPE_STRING,
            'topicId'      => BinarySchema::TYPE_UUID,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
