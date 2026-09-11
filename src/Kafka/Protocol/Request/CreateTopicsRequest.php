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

use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\CreateTopicsRequestTopic;

/**
 * CreateTopics, version 3: asks the controller to create one or more topics (ApiKey 19, Kafka 0.11)
 *
 * Before this api a topic was created by writing to ZooKeeper - with `kafka-topics.sh`, or implicitly by asking a
 * broker with `auto.create.topics.enable` for the metadata of a topic that does not exist yet. Only the ACTIVE
 * CONTROLLER serves the request: every other broker answers each topic of it with the error code 41 (NotController),
 * see `KafkaApis.handleCreateTopicsRequest` @ 0.11.0.3 and {@see \Protocol\Kafka\Admin\AdminClient::findController()}.
 *
 * <pre>
 *   CreateTopics Request (Version: 1, 2 and 3) => [create_topic_requests] timeout validate_only
 *     create_topic_requests => topic num_partitions replication_factor [replica_assignment] [configs]
 *       topic              => STRING
 *       num_partitions     => INT32
 *       replication_factor => INT16
 *       replica_assignment => partition_id [replicas]
 *         partition_id => INT32
 *         replicas     => INT32
 *       configs            => config_key config_value
 *         config_key   => STRING
 *         config_value => STRING
 *     timeout       => INT32
 *     validate_only => BOOLEAN
 * </pre>
 *
 * Version 1 added the trailing `validate_only` flag, which lets the controller check the request - the topic name,
 * the replica placement and every topic-level option - and answer without creating anything; version 0
 * ({@see CreateTopicsRequestV0}) has no such flag, and the Java client refuses to build a version 0 request with
 * `validateOnly` set at all. Version 2 (KIP-124, Kafka 0.11) left the request alone -
 * `CREATE_TOPICS_REQUEST_V2 = CREATE_TOPICS_REQUEST_V1` in `Protocol.java` @ 0.11.0.3 - and only added the leading
 * `throttle_time_ms` to the answer, so {@see CreateTopicsRequestV1} sends the same bytes as this class.
 *
 * `timeout` is how long the controller waits for the topic to be created on it before it answers. With a timeout of
 * 0 the answer comes back immediately and reports the error code 7 (RequestTimedOut) for every topic that was
 * accepted, while the creation carries on in the background: the request "will trigger topic creation and return
 * immediately", see `AdminManager.createTopics` @ 0.11.0.3.
 *
 * **Kafka 2.0 added version 3** and changed nothing about the bytes: `CREATE_TOPICS_REQUEST_V3 =
 * CREATE_TOPICS_REQUEST_V2` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see CreateTopicsRequestV2} is the same frame with the version field of Kafka 0.11.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v3)"
 */
class CreateTopicsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::CREATE_TOPICS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * Topics to create, indexed by their name
     *
     * @var array<string, CreateTopicsRequestTopic>
     */
    protected array $topics;

    /**
     * @param array<int|string, NewTopic|CreateTopicsRequestTopic> $topics Topics to create
     * @param int    $timeout       How long the controller waits for the topics to be created, in milliseconds
     * @param bool   $validateOnly  Validate the request without creating anything (version 1 and above)
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $topics,
        /**
         * Milliseconds the controller waits for the topics to be created before it answers
         */
        protected readonly int $timeout = 30000,
        /**
         * Whether the request should only be validated instead of creating the topics
         *
         * @since Version 1 of protocol
         */
        protected readonly bool $validateOnly = false,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopics = [];
        foreach ($topics as $topic) {
            $entry                       = $topic instanceof NewTopic
                ? CreateTopicsRequestTopic::fromNewTopic($topic)
                : $topic;
            $packedTopics[$entry->topic] = $entry;
        }
        $this->topics = $packedTopics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'topics'  => ['topic' => CreateTopicsRequestTopic::class],
            'timeout' => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 1) {
            $body['validateOnly'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }
}
