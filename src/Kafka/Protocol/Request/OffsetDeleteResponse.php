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
use Protocol\Kafka\Protocol\Data\OffsetDeleteResponseTopic;

/**
 * OffsetDelete response object, version 0 (key 47, Kafka 2.4, KIP-496)
 *
 * <pre>
 *   OffsetDelete Response (Version: 0) => error_code throttle_time_ms [topics]
 *     error_code       => INT16
 *     throttle_time_ms => INT32
 *     topics           => name [partitions]
 *       name       => STRING
 *       partitions => partition_index error_code
 *         partition_index => INT32
 *         error_code      => INT16
 * </pre>
 *
 * **The top-level error code comes before the throttle time here**, which no other api of this protocol does:
 * `OffsetDeleteResponse.json` @ 2.8.2 declares `ErrorCode` first and `ThrottleTimeMs` second, and the generated
 * `OffsetDeleteResponseData` writes them in that order. It is not a typo of this package - the frames captured
 * from the container show the two fields exactly there.
 *
 * **A top-level error answers no topic at all.** `KafkaApis.handleOffsetDeleteRequest` @ 2.8.2 builds the answer
 * with `getErrorResponse()` as soon as `handleDeleteOffsets` reports a group error, so the `topics` array is
 * empty and the caller learns nothing about the individual partitions. The codes measured on the container are:
 *
 * | Code | Name                        | Meaning                                                                 |
 * |------|-----------------------------|-------------------------------------------------------------------------|
 * | 0    | None                        | Every partition of the request carries its own code                     |
 * | 14   | GroupLoadInProgress         | The coordinator is still reading `__consumer_offsets`                   |
 * | 15   | GroupCoordinatorNotAvailable| The coordinator is shutting down, or the offsets topic is not there yet |
 * | 16   | NotCoordinatorForGroup      | This broker does not coordinate that group                              |
 * | 24   | InvalidGroupId              | The group id is empty or otherwise invalid                              |
 * | 30   | GroupAuthorizationFailed    | The client may not delete offsets of that group                         |
 * | 68   | NonEmptyGroup               | The group has live members and is **not** a `consumer` group            |
 * | 69   | GroupIdNotFound             | The coordinator has never heard of that group                           |
 *
 * The difference between 68 and 86 is the protocol type of the group: a live group whose members speak the
 * `consumer` protocol is answered per partition with 86 (GroupSubscribedToTopic) for the topics its members are
 * subscribed to and deletes the rest, while a live group of any other protocol type falls into the `case _` of
 * `GroupCoordinator.handleDeleteOffsets` and is refused as a whole with 68.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
class OffsetDeleteResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Error code of the group itself, 0 when every partition carries its own code
     */
    public int $errorCode;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Result of every topic the coordinator answered, indexed by the topic name
     *
     * @var array<string, OffsetDeleteResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'      => BinarySchema::TYPE_INT16,
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'topics'         => ['name' => OffsetDeleteResponseTopic::class],
        ];
    }
}
