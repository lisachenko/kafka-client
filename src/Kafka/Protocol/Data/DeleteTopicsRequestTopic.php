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
 * One topic of a DeleteTopics request of version 6, named by its NAME or by its ID
 *
 * <pre>
 *   DeleteTopicState => Name TopicId
 *     Name    => COMPACT_NULLABLE_STRING
 *     TopicId => UUID
 * </pre>
 *
 * `DeleteTopicState` of `DeleteTopicsRequest.json` @ 2.8.2, which Kafka 2.8 introduced with the version 6: the
 * flat `[]TopicNames` of every version below is replaced by this structure, and a topic is named **either** by its
 * name - with the zero id - **or** by the `topic_id` KIP-516 gave it, with a null name. A request that carries
 * both a name and a real id is answered 42 `InvalidRequest`.
 *
 * This client names topics by their name, so the id is {@see self::NO_TOPIC_ID} in everything it sends; the
 * structure exists because the version 6 has no other way of saying "this topic".
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
class DeleteTopicsRequestTopic implements BinarySchemaInterface
{
    /**
     * The `topic_id` of a topic that is named by its name: 16 zero bytes (`Uuid.ZERO_UUID` @ 2.8.2)
     */
    public const string NO_TOPIC_ID = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

    /**
     * Name of the topic to delete, `null` for a topic that is named by its id alone
     */
    public ?string $name;

    /**
     * Id of the topic to delete as the raw 16 bytes of its UUID, {@see self::NO_TOPIC_ID} for a named one
     */
    public string $topicId;

    public function __construct(?string $name, string $topicId = self::NO_TOPIC_ID)
    {
        $this->name    = $name;
        $this->topicId = $topicId;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'    => BinarySchema::TYPE_NULLABLE_STRING,
            'topicId' => BinarySchema::TYPE_UUID,
        ];
    }
}
