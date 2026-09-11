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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\MetadataRequestTopic;
use Protocol\Kafka\Protocol\Data\MetadataRequestTopicV9;

/**
 * This API answers the following questions:
 *
 *      What topics exist?
 *      How many partitions does each topic have?
 *      Which broker is currently the leader for each partition?
 *      What is the host and port for each of these brokers?
 *
 * This is the only request that can be addressed to any broker in the cluster.
 * Since there may be many topics the client can give an optional list of topic names in order to only return metadata
 * for a subset of topics.
 *
 * The metadata returned is at the partition level, but grouped together by topic for convenience and to avoid
 * redundancy. For each partition the metadata contains the information for the leader as well as for all the replicas
 * and the list of replicas that are currently in-sync.
 *
 * <pre>
 *   Metadata Request (Version: 7) => [topics] allow_auto_topic_creation
 *     topics                    => NULLABLE_ARRAY of STRING
 *     allow_auto_topic_creation => BOOLEAN     -- since version 4
 * </pre>
 *
 * The versions 1, 2 and 3 send the very same frame - `METADATA_REQUEST_V2 = METADATA_REQUEST_V1` and
 * `METADATA_REQUEST_V3 = METADATA_REQUEST_V2` in `MetadataRequest.schemaVersions()` @ 1.1.1 - and differ in their
 * ANSWER alone ({@see MetadataRequestV3}, {@see MetadataRequestV2}, {@see MetadataRequestV1}). Version 1 (Kafka 0.10.0) made the
 * topic array NULLABLE, which is the whole point of it: a client can now tell the two intentions apart that version
 * 0 ({@see MetadataRequestV0}) had to express with the same empty array.
 *
 * | topics | frame         | 1.1.1 broker answers                                                        |
 * |--------|---------------|-----------------------------------------------------------------------------|
 * | `null` | `ff ff ff ff` | every topic of the cluster, the internal `__consumer_offsets` included       |
 * | `[]`   | `00 00 00 00` | no topic at all - the brokers of the cluster and an empty topic array        |
 *
 * An empty array creates nothing either: `KafkaApis.handleTopicMetadataRequest` @ 1.1.1 only auto-creates the
 * topics that the request NAMES, so `[]` is the cheapest way to ask a broker for the members of the cluster.
 *
 * **Version 4 (KIP-4, Kafka 0.11) is what finally lets a client ask WITHOUT creating anything.** Until then, a
 * metadata request for a topic that does not exist created it whenever the broker ran with the default
 * `auto.create.topics.enable=true`; the trailing `allow_auto_topic_creation` is the client half of that decision,
 * and `KafkaApis.handleTopicMetadataRequest` @ 1.1.1 auto-creates a named topic only when
 * `config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation` holds. A request below version 4 has no
 * such field and `MetadataRequest.allowAutoTopicCreation()` answers `true` for it, so the older versions behave
 * exactly as they did.
 *
 * **Version 5 (Kafka 1.0, KIP-112/113) sends the very same frame again** - `METADATA_REQUEST_V5` is
 * `METADATA_REQUEST_V4` in `MetadataRequest.schemaVersions()` @ 1.1.1 - and states that the client understands the
 * `offline_replicas` array that the ANSWER gained ({@see MetadataResponse}), which is what
 * {@see MetadataRequestV4} lowers the version constant for.
 *
 * **Version 6 (Kafka 2.0, KIP-219) sends that frame once more** - `MetadataRequest.json` @ 2.8.2 has no field
 * between version 4 and version 8 - and states that the **client** waits out the `throttle_time_ms` of the answer
 * itself, because a throttled request is answered first and the channel is muted afterwards, see
 * {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT}. {@see MetadataRequestV5} keeps version 5, which a
 * 2.8.2 broker throttles in exactly the same way - the version is the promise of the client, not a switch of the
 * broker.
 *
 * **Version 7 (Kafka 2.1, KIP-320) sends that frame once more** and states that the client understands the
 * `leader_epoch` the ANSWER gained, see {@see MetadataResponse}; {@see MetadataRequestV6} lowers the version
 * constant for the answer that carries none.
 *
 * **Version 8 (Kafka 2.3, KIP-430) appended the two booleans** of the authorized operations, **version 9 (Kafka
 * 2.4) is the first flexible one** (KIP-482), see {@see self::FLEXIBLE_VERSION}, **version 10 (Kafka 2.8,
 * KIP-516)** put a `topic_id` into every topic entry of the request and of the answer - and left the server side
 * of it unimplemented, see {@see MetadataRequestTopic::$topicId} - and **version 11 (Kafka 2.8, KIP-700) took
 * `include_cluster_authorized_operations` out again**: the cluster-wide question moved to the new DescribeCluster
 * api (key 60), and the answer of version 11 carries no `cluster_authorized_operations` either. This class is
 * version 11; a caller that wants that bitfield from the Metadata api asks with {@see MetadataRequestV10}.
 *
 * The flag is `true` by default here, which is the behaviour of every version below 4 and of
 * {@see \Protocol\Kafka\Common\Cluster}, whose consumers and producers expect a named topic to spring into
 * existence. The administrative side asks with `false`: {@see \Protocol\Kafka\Admin\AdminClient::describeTopics()}
 * and {@see \Protocol\Kafka\Admin\AdminClient::listTopics()} must be able to report that a topic is not there
 * without bringing it into being.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
class MetadataRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::METADATA;

    /**
     * @inheritdoc
     */
    public const int VERSION = 11;

    /**
     * First version of this api whose frame is written with the compact types and the tagged fields of KIP-482
     *
     * `MetadataRequest.json` @ 2.8.2 declares `"flexibleVersions": "9+"`, so a version 9 request carries the
     * request header **v2** - the tag buffer behind the client id - every string as a compact one and a
     * tagged-field section at the end of the body and of every topic entry. Nothing else about the frame
     * changes: version 9 is the version 8 question in the other encoding.
     */
    public const int FLEXIBLE_VERSION = 9;

    /**
     * @param list<string>|null $topics                    Topics to fetch the metadata for, null asks for every topic
     * @param bool              $allowAutoTopicCreation    Whether the broker may create a named topic that does not
     *        exist yet; not on the wire below version 4, where a broker always behaves as if it were true
     * @param string            $clientId                  A user specified identifier for the client
     * @param int               $correlationId             A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        protected ?array $topics = null,
        /**
         * Whether the broker may create a topic that the request names and that does not exist yet.
         *
         * @since Version 4 of protocol
         */
        protected bool $allowAutoTopicCreation = true,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Whether the answer should carry the operations this principal is authorized for on the **cluster**.
         *
         * The bitfield of KIP-430 (Kafka 2.3), see
         * {@see MetadataResponse::$clusterAuthorizedOperations} and {@see \Protocol\Kafka\Common\AclOperation}.
         * `false` - the default of this client, as of the Java `describeCluster` without the option - makes the
         * broker write `Integer.MIN_VALUE` instead, "you did not ask". The field lives in the versions 8 to 10
         * only; KIP-700 moved the question to the DescribeCluster api.
         *
         * @since Version 8 of protocol
         */
        protected bool $includeClusterAuthorizedOperations = false,
        /**
         * Whether every topic entry of the answer should carry the operations this principal is authorized for
         * on **that topic**.
         *
         * The same bitfield, per topic, see {@see \Protocol\Kafka\Common\TopicMetadata::$authorizedOperations}.
         *
         * @since Version 8 of protocol
         */
        protected bool $includeTopicAuthorizedOperations = false
    ) {
        if (static::VERSION >= 9 && $this->topics !== null) {
            // A flexible version writes the topics as structures, see {@see MetadataRequestTopic}; the public
            // shape of this field stays the list of names, which {@see self::getTopics()} hands back
            $topicClass   = static::topicClass();
            $this->topics = array_map(
                static fn(MetadataRequestTopic|string $topic): MetadataRequestTopic
                    => $topic instanceof MetadataRequestTopic ? $topic : new $topicClass($topic),
                $this->topics
            );
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $topics = static::VERSION >= 1
            ? [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true]
            : [BinarySchema::TYPE_STRING];

        // From version 9 - the first flexible one - a topic entry is a STRUCTURE of the specification and gets
        // the tagged-field section that closes every structure of a flexible version, so the array can not be a
        // list of bare strings any more, see {@see MetadataRequestTopic}
        if (static::VERSION >= 9) {
            $topics = [static::topicClass(), BinarySchema::FLAG_NULLABLE => true];
        }

        $body = ['topics' => $topics];
        if (static::VERSION >= 4) {
            $body['allowAutoTopicCreation'] = BinarySchema::TYPE_BOOLEAN;
        }
        // The cluster-wide question of KIP-430 lives in the versions 8 to 10 only: `MetadataRequest.json`
        // @ 2.8.2 declares it as "8-10", because KIP-700 gave it to the DescribeCluster api in version 11
        if (static::VERSION >= 8 && static::VERSION <= 10) {
            $body['includeClusterAuthorizedOperations'] = BinarySchema::TYPE_BOOLEAN;
        }
        if (static::VERSION >= 8) {
            $body['includeTopicAuthorizedOperations'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class sends
     *
     * @return class-string<MetadataRequestTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 10 ? MetadataRequestTopic::class : MetadataRequestTopicV9::class;
    }

    /**
     * Returns the list of topics this request asks the metadata for, null means "every topic"
     *
     * @return list<string>|null
     */
    public function getTopics(): ?array
    {
        if ($this->topics === null) {
            return null;
        }

        return array_map(
            static fn(MetadataRequestTopic|string $topic): string
                => $topic instanceof MetadataRequestTopic ? $topic->name : $topic,
            array_values($this->topics)
        );
    }

    /**
     * Returns whether the broker may create a topic this request names, which only version 4 puts on the wire
     */
    public function isAutoTopicCreationAllowed(): bool
    {
        return $this->allowAutoTopicCreation;
    }
}
