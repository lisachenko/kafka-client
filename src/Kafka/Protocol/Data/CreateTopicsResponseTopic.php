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
use Protocol\Kafka\Protocol\TaggedField;

/**
 * The result of creating one topic, version 5 of the CreateTopics API
 *
 * <pre>
 *   CreateTopicsResponseTopic => Topic ErrorCode ErrorMessage
 *                                 NumPartitions ReplicationFactor [Configs]   -- since version 5
 *                                 [TopicConfigErrorCode]                      -- tagged field 0, since version 5
 *     Topic             => string
 *     ErrorCode         => int16
 *     ErrorMessage      => nullable string
 *     NumPartitions     => int32              -- since version 5, -1 when the broker does not know it
 *     ReplicationFactor => int16              -- since version 5, -1 when the broker does not know it
 *     Configs           => name value read_only config_source is_sensitive   -- NULLABLE array, since version 5
 * </pre>
 *
 * `TOPIC_ERROR` in `Protocol.java` @ 0.10.2.2, which "improves on TOPIC_ERROR_CODE by adding an error_message to
 * complement the error_code": version 0 of the api answers with the bare `Topic ErrorCode` of
 * {@see CreateTopicsResponseTopicV0}, version 1 adds the message that the broker logged for itself before. The
 * message is null whenever the broker has none - for a topic that was created without an error, and for the error
 * codes that `KafkaApis.handleCreateTopicsRequest` produces without an exception (41 NotController and 31
 * ClusterAuthorizationFailed).
 *
 * **Kafka 2.4 added the three fields of KIP-525 with the version 5**: the partition count and the replication
 * factor the topic really got - which is what a client that sent the -1/-1 of KIP-464 wants to know - and the
 * whole configuration of the new topic, so that no DescribeConfigs has to follow. The array is nullable, and the
 * reason it is null travels in the **tagged** field 0 `topic_config_error_code`: the version 5 is the first
 * flexible one of the api, so a tagged field is what a later release adds without another version.
 * {@see CreateTopicsResponseTopicV1} is the entry of the versions 1 to 4.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v6)"
 */
class CreateTopicsResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the CreateTopics API that this DTO is unpacked from
     */
    public const int VERSION = 5;

    /**
     * Value of `num_partitions` and `replication_factor` when the broker did not report them
     *
     * The `"default": "-1"` of both fields in `CreateTopicsResponse.json` @ 2.8.2, which an answer of a version
     * below 5 carries as well, because it has no such field at all.
     */
    public const int UNKNOWN = -1;

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
     * Number of partitions the topic was created with, {@see self::UNKNOWN} when the broker did not say
     *
     * @since Version 5 of protocol
     */
    public int $numPartitions = self::UNKNOWN;

    /**
     * Replication factor of every partition of the topic, {@see self::UNKNOWN} when the broker did not say
     *
     * @since Version 5 of protocol
     */
    public int $replicationFactor = self::UNKNOWN;

    /**
     * Configuration the new topic ended up with, indexed by the option name; **null** when the broker could not
     * read it back, in which case {@see self::$topicConfigErrorCode} says why
     *
     * @since Version 5 of protocol
     *
     * @var array<string, CreateTopicsResponseTopicConfig>|null
     */
    public ?array $configs = null;

    /**
     * Why the configuration above is null, as the **tagged** field 0 of the entry reports it
     *
     * @since Version 5 of protocol
     */
    public int $topicConfigErrorCode = 0;

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
        if (static::VERSION >= 5) {
            $scheme['numPartitions']     = BinarySchema::TYPE_INT32;
            $scheme['replicationFactor'] = BinarySchema::TYPE_INT16;
            $scheme['configs']           = [
                'name'                      => CreateTopicsResponseTopicConfig::class,
                BinarySchema::FLAG_NULLABLE => true,
            ];
            $scheme['topicConfigErrorCode'] = new TaggedField(0, BinarySchema::TYPE_INT16, 0);
        }

        return $scheme;
    }
}
