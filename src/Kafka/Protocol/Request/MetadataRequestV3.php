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

/**
 * Metadata request of version 3 (Kafka 0.11), the nullable topic array without the auto-creation flag
 *
 * <pre>
 *   Metadata Request (Version: 3) => [topics]
 *     topics => NULLABLE_ARRAY of STRING
 * </pre>
 *
 * `METADATA_REQUEST_V3 = METADATA_REQUEST_V2` in `Protocol.java` @ 0.11.0.3, so the frame of this class differs from
 * the one of {@see MetadataRequestV2} in the version field of the header alone; what version 3 changed is the
 * ANSWER, which gained the leading `throttle_time_ms` ({@see MetadataResponseV3}). The
 * `allow_auto_topic_creation` of {@see MetadataRequest} is not on the wire here, and a 0.11.0.3 broker treats every
 * request below version 4 as if it were `true`: `KafkaApis.handleTopicMetadataRequest` @ 0.11.0.3 computes
 * `allowAutoCreation = config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation`, and
 * `MetadataRequest.allowAutoTopicCreation()` answers `true` for a struct without the field.
 *
 * @see docs/protocol/1.1.md, section "Metadata API (key 3, v0 to v4)"
 */
final class MetadataRequestV3 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The `allow_auto_topic_creation` of version 4 is not on the wire in this version, and a broker behaves as if
     * it were `true`; the constructor therefore does not ask for it.
     *
     * @param list<string>|null $topics        Topics to fetch the metadata for, null asks for every topic
     * @param string            $clientId      A user specified identifier for the client making the request
     * @param int               $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(?array $topics = null, string $clientId = '', int $correlationId = 0)
    {
        parent::__construct($topics, true, $clientId, $correlationId);
    }
}
