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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DeleteShareGroupOffsetsResponseTopic;

/**
 * What the group coordinator made of a deletion of share-group state (ApiKey 92, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DeleteShareGroupOffsets Response (Version: 0) => ThrottleTimeMs ErrorCode ErrorMessage [Responses] TAG_BUFFER
 *     ThrottleTimeMs => INT32
 *     ErrorCode      => INT16
 *     ErrorMessage   => COMPACT_NULLABLE_STRING
 *     Responses      => COMPACT_ARRAY of {@see DeleteShareGroupOffsetsResponseTopic}
 * </pre>
 *
 * Two levels of error: the top-level pair for the group - the **69** `GroupIdNotFound` of a group the coordinator
 * does not hold, the **68** `NonEmptyGroup` of one with members - and one code per topic, the 3
 * `UnknownTopicOrPartition` among the supported errors of `DeleteShareGroupOffsetsResponse.json` @ 4.1.0.
 *
 * @see docs/protocol/4.3.md, section "DeleteShareGroupOffsets API (key 92, v0)"
 */
class DeleteShareGroupOffsetsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the group as a whole, 0 when the topics carry the results
     */
    public int $errorCode = 0;

    /**
     * Human readable description of the top-level error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Result of every topic, indexed by the topic name
     *
     * @var array<string, DeleteShareGroupOffsetsResponseTopic>
     */
    public array $responses = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'responses'      => ['topicName' => DeleteShareGroupOffsetsResponseTopic::class],
        ];
    }
}
