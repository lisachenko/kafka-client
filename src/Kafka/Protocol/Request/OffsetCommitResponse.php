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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetCommitResponseTopicV0;

/**
 * Offset commit response object, version 10
 *
 * <pre>
 *   OffsetCommit Response (Version: 10) => throttle_time_ms [responses]
 *     responses => topic_id [partition_responses]   -- the topic id of KIP-848 in place of the name
 *       topic_id => UUID
 *
 *   OffsetCommit Response (Version: 3 to 6) => throttle_time_ms [responses]
 *     throttle_time_ms => INT32     -- since version 3
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition error_code
 *         partition  => INT32
 *         error_code => INT16
 * </pre>
 *
 * The versions 0, 1 and 2 answer the topics array and nothing else - `OFFSET_COMMIT_RESPONSE_V1 =
 * OFFSET_COMMIT_RESPONSE_V2 = OFFSET_COMMIT_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3 - and version 3 (KIP-124,
 * Kafka 0.11) put a `throttle_time_ms` in front of it. Version 4 (KIP-219, Kafka 2.0) did not touch the answer
 * either, it only changed *when* a throttled broker sends it. {@see OffsetCommitResponseV3},
 * {@see OffsetCommitResponseV2}, {@see OffsetCommitResponseV1} and {@see OffsetCommitResponseV0} lower the version
 * constant this scheme follows, and so do {@see OffsetCommitResponseV4} and {@see OffsetCommitResponseV5}: the
 * answer is one and the same layout from version 3 on, because neither KIP-211 nor KIP-320 touched it.
 *
 * **Version 9 (Kafka 3.6, KIP-848) does not touch it either** - "the response is the same as version 8" in
 * `OffsetCommitResponse.json` @ 3.6.2 - but it carries two error codes the versions below it cannot: the **69**
 * `GroupIdNotFound` of a group the coordinator does not know, which a version 8 answer reports as the **22**
 * `IllegalGeneration`, and the **113** `StaleMemberEpoch` of a member of a KIP-848 group whose member epoch is
 * behind the one the coordinator holds. {@see OffsetCommitResponseV8} keeps the version below it.
 *
 * **Version 10 (Kafka 4.2, KIP-848) names every topic by its id**: `OffsetCommitResponse.json` @ 4.2.0 declares
 * `Name` as `"versions": "0-9"` and `TopicId` as `"10+"`, so {@see self::$topics} is a list of entries that carry the
 * `topic_id` and the empty name ({@see OffsetCommitResponseTopic}), and a partition of an id the node does not know
 * carries the **100** `UNKNOWN_TOPIC_ID` - a code the JSON lists for version 10 alone. {@see self::topicsByName()}
 * maps the entries back to the names the request committed for. {@see OffsetCommitResponseV9} keeps the answer
 * that names its topics.
 *
 * @see docs/protocol/4.3.md, sections "OffsetCommit API (key 8, v0 to v10)" and "Quotas and throttle time"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetCommit (v10, KIP-848)"
 */
class OffsetCommitResponse extends AbstractResponse
{
    /**
     * Version of the OffsetCommit API that this class decodes the answer of
     */
    public const int VERSION = 10;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 8;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 3 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * List of topics with the result for each of their partitions, indexed by the topic name - a list from version
     * 10 on, whose entries name the topic by its id
     *
     * @var array<array-key, OffsetCommitResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 3) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        // From version 10 the entries carry no name, so there is no field to index the array by
        $body['topics'] = static::VERSION >= OffsetCommitRequest::MIN_TOPIC_ID_VERSION
            ? [OffsetCommitResponseTopic::class]
            : ['topic' => OffsetCommitResponseTopicV0::class];

        return $header + $body;
    }

    /**
     * Returns the topics of the answer indexed by their name, whatever version the answer is
     *
     * An answer below version 10 is indexed by name already. An entry of a version 10 answer is named with the map of
     * the request - the names and the ids this client committed with - and an id the map does not hold, which a node
     * never answers, keeps the text form of its uuid as its name.
     *
     * @param array<string, string> $topicIds Id of every topic of the request, as name => the 16 raw bytes of its uuid
     *
     * @return array<string, OffsetCommitResponseTopic>
     */
    public function topicsByName(array $topicIds = []): array
    {
        if (static::VERSION < OffsetCommitRequest::MIN_TOPIC_ID_VERSION) {
            return $this->topics;
        }

        $namesById = array_flip($topicIds);
        $topics    = [];
        foreach ($this->topics as $topic) {
            if ($topic->topic === '') {
                $topic->topic = (string) ($namesById[$topic->topicId] ?? Uuid::toString($topic->topicId));
            }
            $topics[$topic->topic] = $topic;
        }

        return $topics;
    }
}
