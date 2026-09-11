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

/**
 * DeleteTopics, version 1: asks the controller to delete one or more topics (ApiKey 20, Kafka 0.11)
 *
 * <pre>
 *   DeleteTopics Request (Version: 0 and 1) => [topics] timeout
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
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 and v1)"
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
    public const int VERSION = 1;

    /**
     * @param list<string> $topics        Names of the topics to delete
     * @param int          $timeout       How long the controller waits for the deletion, in milliseconds
     * @param string       $clientId      A user specified identifier for the client making the request
     * @param int          $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * Names of the topics to delete
         *
         * @var list<string>
         */
        protected readonly array $topics,
        /**
         * Milliseconds the controller waits for the topics to be deleted before it answers
         */
        protected readonly int $timeout = 30000,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics'  => [BinarySchema::TYPE_STRING],
            'timeout' => BinarySchema::TYPE_INT32,
        ];
    }
}
