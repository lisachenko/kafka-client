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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DeleteTopicsRequestTopic;

/**
 * DeleteTopics, version 3: asks the controller to delete one or more topics (ApiKey 20, Kafka 0.11)
 *
 * <pre>
 *   DeleteTopics Request (Version: 0, 1 and 2) => [topics] timeout
 *     topics  => STRING
 *     timeout => INT32
 * </pre>
 *
 * `DELETE_TOPICS_REQUEST_V1 = DELETE_TOPICS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124, Kafka
 * 0.11) added the leading `throttle_time_ms` to the ANSWER alone ({@see DeleteTopicsResponse}), so
 * {@see DeleteTopicsRequestV0} sends the same bytes.
 *
 * Like {@see CreateTopicsRequest} this is served by the ACTIVE CONTROLLER only, and every other broker answers each
 * topic of the request with the error code 41 (NotController) - with one exception that
 * {@see \Protocol\Kafka\Admin\AdminClient::findController()} had to work around: the answer holds one entry per
 * REQUESTED topic, so a request with an EMPTY topic array is answered with an empty array by every broker of the
 * cluster, the controller and the followers alike (`KafkaApis.handleDeleteTopicsRequest` @ 0.10.2.2 maps
 * `deleteTopicRequest.topics`, which is empty).
 *
 * Deleting a topic is asynchronous even on the controller: `AdminUtils.deleteTopic` only writes the topic into the
 * `/admin/delete_topics` path of ZooKeeper, and the controller then removes the partitions from the brokers. The
 * `timeout` is how long the controller waits for that to finish before it answers; with a timeout of 0 the answer
 * comes back immediately with the error code 7 (RequestTimedOut) for every topic that was accepted, while the
 * deletion carries on. A topic that is already marked for deletion is accepted again without an error
 * (`TopicAlreadyMarkedForDeletionException` is swallowed by `AdminManager.deleteTopics`), a topic the broker does
 * not know at all is answered with 3 (UnknownTopicOrPartition), and a broker that runs with
 * `delete.topic.enable=false` - the default of Kafka 0.10 - never carries the deletion out at all.
 *
 * **Kafka 2.0 added version 2** and changed nothing about the bytes: `DELETE_TOPICS_REQUEST_V2 =
 * DELETE_TOPICS_REQUEST_V1` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see DeleteTopicsRequestV1} is the same frame with the version field of Kafka 0.11.
 *
 *
 * **Kafka 2.1 added version 3** with the very same frame once more (`DELETE_TOPICS_REQUEST_V3 =
 * DELETE_TOPICS_REQUEST_V2` in `Protocol.java` @ 2.1.1, whose comment says "v3 request is the same that as v2. The
 * response is different based on the request version. In v3 version a TopicDeletionDisabledException is
 * returned"). The version is what a client uses to say that it understands the error code **73**
 * `TOPIC_DELETION_DISABLED`: `KafkaApis.handleDeleteTopicsRequest` @ 2.8.2 answers a cluster whose
 * `delete.topic.enable` is false with `if (request.context.apiVersion < 3) Errors.INVALID_REQUEST else
 * Errors.TOPIC_DELETION_DISABLED`, so a version 2 client keeps getting the 42 of the lines below and a version 3
 * client is told what is really wrong. The option is not dynamic and is `true` on the container of this line, so
 * the code is documented from the sources rather than measured. {@see DeleteTopicsRequestV2} is the same frame
 * with the version field of Kafka 2.0.
 *
 * **Kafka 2.4 added the version 4** (KIP-482) and changed no field at all: it is the **first flexible version** of
 * this api, so every string and every array of it is written compactly and every structure ends in a tagged-field
 * section. The engine does that from {@see self::FLEXIBLE_VERSION} alone; {@see DeleteTopicsRequestV3} is the same
 * body in the encoding of Kafka 2.1.
 *
 * **Kafka 2.7 added the version 5** (KIP-599) with the same two fields once more: the version is what the client
 * promises the controller mutation quota with - it takes the error code 89 `ThrottlingQuotaExceeded` and repeats
 * the topics itself - and the answer gains an `error_message` per topic. {@see DeleteTopicsRequestV4} is the frame
 * of Kafka 2.4.
 *
 * **Kafka 2.8 rebuilt the request with the version 6** (KIP-516): the flat `[]TopicNames` becomes a
 * `[]DeleteTopicState`, a structure that names a topic by its **name** - with the zero id - or by the `topic_id`
 * the controller gave it, with a null name; a request that carries both is answered 42 `InvalidRequest` (measured on the container).
 * {@see DeleteTopicsRequestTopic} is that structure, {@see DeleteTopicsRequestV5} the flat frame of Kafka 2.7.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
class DeleteTopicsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DELETE_TOPICS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 6;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 4;

    /**
     * Names of the topics to delete, as the versions below 6 write them
     *
     * A topic that the caller named by its id alone is `null` here, which is what the version 6 allows and no
     * version below it can express.
     *
     * @var list<string|null>
     */
    protected readonly array $topics;

    /**
     * The topics of a version 6 request, one entry per topic of the request
     *
     * @var list<DeleteTopicsRequestTopic>
     *
     * @since Version 6 of protocol
     */
    protected readonly array $topicStates;

    /**
     * @param list<string|DeleteTopicsRequestTopic> $topics        Topics to delete, each named by its name or,
     *                                                             with a {@see DeleteTopicsRequestTopic}, by its id
     * @param int                                   $timeout       Milliseconds the controller waits for the
     *                                                             deletion before it answers
     * @param string                                $clientId      Identifier of the client making the request
     * @param int                                   $correlationId Value the broker passes back unmodified
     */
    public function __construct(
        array $topics,
        /**
         * Milliseconds the controller waits for the topics to be deleted before it answers
         */
        protected readonly int $timeout = 30000,
        string $clientId = '',
        int $correlationId = 0
    ) {
        // Kafka 2.8 replaced the flat name array by a structure that names a topic by its name OR by its id; a
        // topic this client deletes is a named one unless the caller built the entry itself, and a named entry
        // carries the zero id
        $states = [];
        $names  = [];
        foreach ($topics as $topic) {
            $state    = $topic instanceof DeleteTopicsRequestTopic ? $topic : new DeleteTopicsRequestTopic($topic);
            $states[] = $state;
            $names[]  = $state->name;
        }

        $this->topicStates = $states;
        $this->topics      = $names;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        $body = static::VERSION >= 6
            ? ['topicStates' => [DeleteTopicsRequestTopic::class]]
            : ['topics' => [BinarySchema::TYPE_STRING]];
        $body['timeout'] = BinarySchema::TYPE_INT32;

        return $header + $body;
    }
}
