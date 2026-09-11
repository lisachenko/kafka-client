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

/**
 * The result of one topic of a CreatePartitions answer, i.e. one entry of the `topic_errors` array
 *
 * <pre>
 *   CreatePartitionsResponseTopic => topic error_code error_message
 *     topic         => STRING
 *     error_code    => INT16
 *     error_message => NULLABLE_STRING
 * </pre>
 *
 * `CREATE_PARTITIONS_RESPONSE_V0` of `CreatePartitionsResponse.java` @ 1.1.1, whose two error fields are the shared
 * `ApiError` of the Java client - the same pair a CreateTopics v1 answer carries, in the same order, but behind the
 * topic name instead of in front of it.
 *
 * The error is per topic: one topic that could not grow does not spoil the others of the request, and the message is
 * the only place that says why - `Topic already has 5 partitions.`, `Increasing the number of partitions by adding
 * more replicas is not supported.`, and so on.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 to v3)"
 */
class CreatePartitionsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this result belongs to
     */
    public string $topic;

    /**
     * Error code of this topic, 0 when its partition count was raised (or validated)
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'        => BinarySchema::TYPE_STRING,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
