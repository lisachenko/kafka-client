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
 * One topic of the `forgotten_topics_data` array of a Fetch request of version 7 (KIP-227, Kafka 1.1)
 *
 * <pre>
 *   FetchRequestForgottenTopic => TopicName [Partition]
 *     TopicName => string
 *     Partition => int32
 * </pre>
 *
 * The array tells a fetch session which partitions it should drop: `FetchSession.update` @ 1.1.1 removes every
 * partition named here from the cached partition map of the session, so the broker stops answering for it and the
 * client stops paying for it in every following incremental fetch. It is the counterpart of the topics array,
 * which *adds* partitions to a session or updates their fetch parameters.
 *
 * A session-less request and a full fetch carry an empty array - the broker ignores it in both cases, because
 * there is no session to remove anything from - and the field does not exist below version 7 at all. `Schema
 * FORGOTTEN_TOPIC_DATA` @ 1.1.1 is the entry of that array; the field itself is spelled `forgetten_topics_data`
 * in `FetchRequest.java`, a typo that never reached the wire because a Kafka request header carries no field names.
 *
 * **Version 13 (Kafka 3.1, KIP-516) names the topic by its id here as well**: `FetchRequest.json` @ 3.1.2
 * declares the `Topic` of this entry as `versions 7-12` and its `TopicId` as `13+`, exactly as it does for the
 * topics array, because a session of that version is keyed by ids and a partition can only be dropped from it
 * under the name the session knows it by. {@see FetchRequestForgottenTopicV7} keeps the entry that names it by
 * its name.
 *
 * @see \Protocol\Kafka\Protocol\Request\FetchMetadata
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v13)", "Fetch sessions (v7, KIP-227)" and
 *      "The topic ids of the fetch path (v13, KIP-516)"
 */
class FetchRequestForgottenTopic implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO is packed for
     */
    public const int VERSION = 13;

    /**
     * Name of the topic whose partitions the session should forget
     */
    public string $topic;

    /**
     * Id of that topic, the 16 raw bytes of the `uuid` of KIP-516, {@see Uuid::ZERO} below version 13
     *
     * @since Version 13 of protocol (Kafka 3.1, KIP-516)
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Partitions of that topic to remove from the session
     *
     * @var list<int>
     */
    public array $partitions;

    /**
     * @param list<int> $partitions Partitions to remove from the fetch session
     */
    public function __construct(string $topic, array $partitions = [], string $topicId = Uuid::ZERO)
    {
        $this->topic      = $topic;
        $this->partitions = $partitions;
        $this->topicId    = $topicId;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = static::VERSION >= 13
            ? ['topicId' => BinarySchema::TYPE_UUID]
            : ['topic' => BinarySchema::TYPE_STRING];

        $scheme['partitions'] = [BinarySchema::TYPE_INT32];

        return $scheme;
    }
}
