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
 *   Metadata Request (Version: 4) => [topics] allow_auto_topic_creation
 *     topics                    => NULLABLE_ARRAY of STRING
 *     allow_auto_topic_creation => BOOLEAN     -- since version 4
 * </pre>
 *
 * The versions 1, 2 and 3 send the very same frame - `METADATA_REQUEST_V2 = METADATA_REQUEST_V1` and
 * `METADATA_REQUEST_V3 = METADATA_REQUEST_V2` in `Protocol.java` @ 0.11.0.3 - and differ in their ANSWER alone
 * ({@see MetadataRequestV3}, {@see MetadataRequestV2}, {@see MetadataRequestV1}). Version 1 (Kafka 0.10.0) made the
 * topic array NULLABLE, which is the whole point of it: a client can now tell the two intentions apart that version
 * 0 ({@see MetadataRequestV0}) had to express with the same empty array.
 *
 * | topics | frame         | 0.11.0.3 broker answers                                                     |
 * |--------|---------------|-----------------------------------------------------------------------------|
 * | `null` | `ff ff ff ff` | every topic of the cluster, the internal `__consumer_offsets` included       |
 * | `[]`   | `00 00 00 00` | no topic at all - the brokers of the cluster and an empty topic array        |
 *
 * An empty array creates nothing either: `KafkaApis.handleTopicMetadataRequest` @ 0.11.0.3 only auto-creates the
 * topics that the request NAMES, so `[]` is the cheapest way to ask a broker for the members of the cluster.
 *
 * **Version 4 (KIP-4, Kafka 0.11) is what finally lets a client ask WITHOUT creating anything.** Until then, a
 * metadata request for a topic that does not exist created it whenever the broker ran with the default
 * `auto.create.topics.enable=true`; the trailing `allow_auto_topic_creation` is the client half of that decision,
 * and `KafkaApis.handleTopicMetadataRequest` @ 0.11.0.3 auto-creates a named topic only when
 * `config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation` holds. A request below version 4 has no
 * such field and `MetadataRequest.allowAutoTopicCreation()` answers `true` for it, so the older versions behave
 * exactly as they did.
 *
 * The flag is `true` by default here, which is the behaviour of every version below 4 and of
 * {@see \Protocol\Kafka\Common\Cluster}, whose consumers and producers expect a named topic to spring into
 * existence. The administrative side asks with `false`: {@see \Protocol\Kafka\Admin\AdminClient::describeTopics()}
 * and {@see \Protocol\Kafka\Admin\AdminClient::listTopics()} must be able to report that a topic is not there
 * without bringing it into being.
 *
 * @see docs/protocol/0.11.0.md, section "Metadata API (key 3, v0 to v4)"
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
    public const int VERSION = 4;

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
        $topics = static::VERSION >= 1
            ? [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true]
            : [BinarySchema::TYPE_STRING];

        $body = ['topics' => $topics];
        if (static::VERSION >= 4) {
            $body['allowAutoTopicCreation'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }

    /**
     * Returns the list of topics this request asks the metadata for, null means "every topic"
     *
     * @return list<string>|null
     */
    public function getTopics(): ?array
    {
        return $this->topics;
    }

    /**
     * Returns whether the broker may create a topic this request names, which only version 4 puts on the wire
     */
    public function isAutoTopicCreationAllowed(): bool
    {
        return $this->allowAutoTopicCreation;
    }
}
