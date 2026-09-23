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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseGroup;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopicV0;
use UnexpectedValueException;

/**
 * OffsetFetch response object, version 9
 *
 * <pre>
 *   OffsetFetch Response (Version: 5 to 7) => throttle_time_ms [responses] error_code
 *     throttle_time_ms => INT32     -- since version 3
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition offset leader_epoch metadata error_code
 *         partition    => INT32
 *         offset       => INT64
 *         leader_epoch => INT32            -- since version 5
 *         metadata     => NULLABLE_STRING
 *         error_code   => INT16
 *     error_code => INT16           -- since version 2
 *
 *   OffsetFetch Response (Version: 8, 9) => throttle_time_ms [groups]
 *     groups => group_id [responses] error_code   -- since version 8, one entry per group of the request
 * </pre>
 *
 * Version 2 appended a **group-level** `error_code` **after** the topics array. It reports what is wrong with the
 * group rather than with one of its partitions - 15 (GroupCoordinatorNotAvailable), 16 (NotCoordinatorForGroup), 14
 * (GroupLoadInProgress) or 30 (GroupAuthorizationFailed) - and the answer that carries it has an **empty** topics
 * array: `OffsetFetchRequest.getErrorResponse()` @ 0.11.0.3 fills the partitions only for the versions below 2,
 * which had nowhere else to put a group error. The per-partition codes stay what they were.
 *
 * Version 3 (KIP-124, Kafka 0.11) added the leading `throttle_time_ms` and changed nothing else, so this answer
 * carries an error code at each of its two ends: the throttle time, the topics, and then the group error. Version 4
 * (KIP-219, Kafka 2.0) is the same answer one api version higher, and **version 5 (KIP-320, Kafka 2.1)** inserts
 * the `committed_leader_epoch` into every partition entry, between the committed offset and the metadata. The
 * class of a partition follows the version of the topic entry, so {@see OffsetFetchResponseV4} and the versions
 * below it decode their answers through {@see \Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartitionV0}.
 *
 * The lower versions each have a class of their own: {@see OffsetFetchResponseV2} still reads the group error code,
 * {@see OffsetFetchResponseV1} and {@see OffsetFetchResponseV0} do not have it and report
 * {@see KafkaException::NO_ERROR} here, because the whole answer of those versions is made of per-partition
 * results.
 *
 * **Versions 6 (KIP-482, Kafka 2.4) and 7 (KIP-447, Kafka 2.5) changed no field of this answer**: the first is
 * the flexible encoding of the very same layout, and the second is a promise about an error code - a partition of
 * a version 7 answer can carry the retriable **88** (`UnstableOffsetCommit`) when the request asked for stable
 * offsets and the last commit of that partition belongs to a transaction that is still open.
 * {@see OffsetFetchResponseV6} decodes the same bytes one api version lower.
 *
 * **Version 8 (Kafka 3.0) answers one entry per group.** `OffsetFetchResponse.json` @ 3.0.2 ends `Topics` and the
 * top-level `ErrorCode` at the version 7 and puts an array of {@see OffsetFetchResponseGroup} behind the throttle
 * time instead, each entry with the group id it answers, the topics of that group and a group-level error code of
 * its own - so a batch of three groups reports three error codes and **no** top-level one at all. The topics and
 * the partitions inside an entry are byte for byte the ones of version 7, `committed_leader_epoch` included.
 * {@see self::groupOf()} reads one group out of either shape, and {@see OffsetFetchResponseV7} decodes the answer
 * of the versions below.
 *
 * **Version 9 (Kafka 3.7, KIP-848) changed no field of this half either**: "the response is the same as version 8
 * but can return STALE_MEMBER_EPOCH and UNKNOWN_MEMBER_ID errors when the new consumer group protocol is used"
 * (`OffsetFetchResponse.json` @ 3.7.2). The two codes stand in the **group-level** `error_code` of the entry
 * whose request named a member id and a member epoch: **25** `UnknownMemberId` for a member the KIP-848 group
 * does not hold and **113** `StaleMemberEpoch` for an epoch that is not the one the coordinator holds for it.
 * The topics of such an entry are empty, exactly as they are for every other group-level error of this api.
 * {@see OffsetFetchResponseV8} decodes the same bytes one api version lower.
 *
 * @see docs/protocol/4.3.md, sections "OffsetFetch API (key 9, v0 to v9)", "Stable offsets and the 88 of KIP-447
 *      (Kafka 2.5)" and "Quotas and throttle time"
 * @see docs/protocol/4.3.md, section "The member id and epoch of KIP-848 (v9)"
 */
class OffsetFetchResponse extends AbstractResponse
{
    /**
     * Version of the OffsetFetch API that this class decodes the answer of
     */
    public const int VERSION = 9;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 6;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 3 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * List of topic responses
     *
     * Only the versions below 8 carry it at the top level; a batched answer names the topics of each group of its
     * `groups` array instead.
     *
     * @var array<string, OffsetFetchResponseTopic>
     */
    public array $topics = [];

    /**
     * Error of the group itself, which the coordinator reports instead of any topic at all
     *
     * A version 8 answer has no top-level error code at all: every group of the batch carries its own.
     *
     * @since Version 2 of protocol
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * The answer of every group of a batched request, indexed by the group id
     *
     * @since Version 8 of protocol
     *
     * @var array<string, OffsetFetchResponseGroup>
     */
    public array $groups = [];

    /**
     * Returns the answer for one group, whatever version of the api this answer is
     *
     * A version 8 answer is asked for the entry of that group; anything below it answers one group at its top
     * level and the given id only fills {@see OffsetFetchResponseGroup::$groupId} of the entry this builds, so
     * that a caller reads both shapes the same way.
     *
     * @throws UnexpectedValueException If a batched answer carries no entry for the given group
     */
    public function groupOf(string $groupId): OffsetFetchResponseGroup
    {
        if (static::VERSION >= OffsetFetchRequest::MIN_BATCHED_VERSION) {
            return $this->groups[$groupId] ?? throw new UnexpectedValueException(
                "The OffsetFetch answer carries no entry for the group '{$groupId}'"
            );
        }

        return OffsetFetchResponseGroup::of($groupId, $this->topics, $this->errorCode);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 3) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        if (static::VERSION >= OffsetFetchRequest::MIN_BATCHED_VERSION) {
            $body['groups'] = ['groupId' => OffsetFetchResponseGroup::class];

            return $header + $body;
        }

        $body['topics'] = ['topic' => static::topicClass()];
        if (static::VERSION >= 2) {
            $body['errorCode'] = BinarySchema::TYPE_INT16;
        }

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class decodes
     *
     * @return class-string<OffsetFetchResponseTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 5 ? OffsetFetchResponseTopic::class : OffsetFetchResponseTopicV0::class;
    }
}
