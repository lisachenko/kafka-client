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
 * Metadata request of version 0, the one every Kafka release from 0.8 on serves
 *
 * <pre>
 *   Metadata Request (Version: 0) => [topics]
 *     topics => ARRAY of STRING
 * </pre>
 *
 * The topic array of `METADATA_REQUEST_V0` is NOT nullable, so this version has no way of saying "no topic": an
 * EMPTY array asks for every topic of the cluster, and `KafkaApis.handleTopicMetadataRequest` @ 0.10.2.2 spells
 * that out - "Handle old metadata request logic. Version 0 has no way to specify 'no topics'". A `null` handed to
 * the constructor therefore means the same as an empty array here and is written as `00 00 00 00`.
 *
 * The answer to this version is the one of {@see MetadataResponseV0}, without the cluster id, the controller id,
 * the racks of the brokers and the internal flag of the topics. One more thing sets it apart from the later
 * versions: a version 0 answer reports 9 (ReplicaNotAvailable) for a partition whose replica set contains a broker
 * that is currently down, while version 1 and above simply leave that broker out of the replica list
 * (`errorUnavailableEndpoints = requestVersion == 0` in `KafkaApis` @ 0.10.2.2).
 *
 * @see docs/protocol/1.1.md, section "Metadata API (key 3, v0 to v5)"
 */
final class MetadataRequestV0 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param list<string>|null $topics        Topics to fetch the metadata for, empty asks for every topic
     * @param string            $clientId      A user specified identifier for the client making the request
     * @param int               $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(?array $topics = [], string $clientId = '', int $correlationId = 0)
    {
        // The `allow_auto_topic_creation` of version 4 is not on the wire here, and a broker treats every request
        // below that version as if it were `true`, which is the value this class passes on
        parent::__construct($topics ?? [], true, $clientId, $correlationId);
    }

    /**
     * Returns the list of topics this request asks the metadata for, empty means "every topic"
     *
     * @return list<string>
     */
    public function getTopics(): array
    {
        return $this->topics ?? [];
    }
}
