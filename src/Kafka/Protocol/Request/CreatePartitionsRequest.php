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

use Protocol\Kafka\Admin\NewPartitions;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\CreatePartitionsRequestTopic;

/**
 * CreatePartitions, version 1: raises the partition count of existing topics (ApiKey 37, Kafka 1.0, KIP-195)
 *
 * <pre>
 *   CreatePartitions Request (Version: 0 and 1) => [topic_partitions] timeout validate_only
 *     topic_partitions => topic count assignment
 *       topic      => STRING
 *       count      => INT32
 *       assignment => NULLABLE_ARRAY of ARRAY of INT32
 *     timeout       => INT32
 *     validate_only => BOOLEAN
 * </pre>
 *
 * KIP-195 is the last piece of `kafka-topics.sh --alter` that still needed ZooKeeper: adding partitions to a topic
 * that exists. Only the ACTIVE CONTROLLER serves the request - `KafkaApis.handleCreatePartitionsRequest` @ 1.1.1
 * answers every topic of it with the error code 41 (NotController) otherwise, exactly like CreateTopics - and the
 * api can only ever GROW a topic: `AdminManager.createPartitions` refuses a count below or equal to the current one
 * with 37 (InvalidPartitions), because Kafka cannot merge the log of two partitions and the keys of a compacted
 * topic would change their partition.
 *
 * `timeout` is how long the controller waits for the new partitions to appear on it before it answers, as in
 * CreateTopics: a timeout of 0 answers immediately with the error code 7 (RequestTimedOut) for every accepted topic
 * while the work carries on. `validate_only` runs the whole validation and writes nothing.
 *
 * The request is the same shape as CreateTopics with one entry per topic, and every entry gets an entry of its own
 * in the answer; a topic that appears TWICE in one request is answered with 42 (InvalidRequest) - the Java class
 * tracks the duplicates itself, see `CreatePartitionsRequest.duplicates`.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `CREATE_PARTITIONS_REQUEST_V1 =
 * CREATE_PARTITIONS_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see CreatePartitionsRequestV0} is the same frame with the version field of Kafka 1.0.
 *
 * **Kafka 2.5 added the version 2** (KIP-482), the same fields in the flexible encoding: every string and array of
 * the frame is compact, the header carries a tag buffer and every structure ends in one. Not a field changed.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 to v3)"
 */
class CreatePartitionsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::CREATE_PARTITIONS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Topics whose partition count should grow, indexed by their name
     *
     * @var array<string, CreatePartitionsRequestTopic>
     */
    protected array $topics;

    /**
     * @param array<string, NewPartitions|int|CreatePartitionsRequestTopic> $topics Topics to grow, by topic name
     * @param int    $timeout       How long the controller waits for the partitions to be created, in milliseconds
     * @param bool   $validateOnly  Validate the request without adding anything
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $topics,
        /**
         * Milliseconds the controller waits for the partitions to be created before it answers
         */
        protected readonly int $timeout = 30000,
        /**
         * Whether the request should only be validated instead of adding the partitions
         */
        protected readonly bool $validateOnly = false,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopics = [];
        foreach ($topics as $topic => $newPartitions) {
            $entry = match (true) {
                $newPartitions instanceof CreatePartitionsRequestTopic => $newPartitions,
                $newPartitions instanceof NewPartitions
                    => CreatePartitionsRequestTopic::fromNewPartitions((string) $topic, $newPartitions),
                default => new CreatePartitionsRequestTopic((string) $topic, $newPartitions),
            };

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

        return $header + [
            'topics'       => ['topic' => CreatePartitionsRequestTopic::class],
            'timeout'      => BinarySchema::TYPE_INT32,
            'validateOnly' => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}
