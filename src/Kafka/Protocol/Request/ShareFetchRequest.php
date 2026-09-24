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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;
use Protocol\Kafka\Protocol\Data\ShareFetchRequestForgottenTopic;
use Protocol\Kafka\Protocol\Data\ShareFetchRequestPartition;
use Protocol\Kafka\Protocol\Data\ShareFetchRequestTopic;

/**
 * ShareFetch, version 1: fetches records of a share group and acquires them for the member (ApiKey 78, Kafka 4.1)
 *
 * <pre>
 *   ShareFetch Request (Version: 1) => group_id member_id share_session_epoch max_wait_ms min_bytes max_bytes
 *                                      max_records batch_size [topics] [forgotten_topics_data]
 *     group_id            => COMPACT_NULLABLE_STRING   -- null is the 42 "Invalid group id in the request."
 *     member_id           => COMPACT_NULLABLE_STRING   -- 1 to 36 characters
 *     share_session_epoch => INT32                     -- 0 opens a share session, -1 closes it, else the next one
 *     max_wait_ms         => INT32
 *     min_bytes           => INT32
 *     max_bytes           => INT32
 *     max_records         => INT32                     -- since version 1
 *     batch_size          => INT32                     -- since version 1
 *     topics              => topic_id [partitions]
 *       partitions => partition_index [acknowledgement_batches]
 *     forgotten_topics_data => topic_id [partitions]
 * </pre>
 *
 * `ShareFetchRequest.json` @ 4.1.0 (`validVersions` `1`; the version 0 of the early access of Kafka 4.0 is gone, and
 * with it the per-partition `partition_max_bytes`). A share fetch is a Fetch of a **share group**: it names no offset
 * - the share-partition of the group decides which records are next - and the broker **acquires** the records it
 * answers for the member, who then accepts, releases or rejects them, in the acknowledgement batches of its next
 * share fetch or in a ShareAcknowledge ({@see ShareAcknowledgeRequest}).
 *
 * The **share session** of a member is the fetch session of KIP-227 for share groups, keyed by group and member id
 * on the broker: the epoch **0** opens it with the partitions of the request (and may carry no acknowledgement - the
 * 42), every following request names the next epoch and only the partitions it adds or acknowledges, and **-1**
 * closes it. A wrong epoch is the **123** `InvalidShareSessionEpoch`, a session the broker does not hold the **122**
 * `ShareSessionNotFound`, and a broker that holds `group.share.max.share.sessions` sessions already refuses a new one
 * with the **133** `ShareSessionLimitReached` (`SharePartitionManager.newContext` @ 4.3.1).
 *
 * `max_records` and `batch_size` are `max.poll.records` of the Java share consumer for both
 * (`ShareSessionHandler` @ 4.1.0), 500 by default.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 */
class ShareFetchRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::SHARE_FETCH;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * The api is flexible from its first version
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Share session epoch that opens a new session (`ShareRequestMetadata.INITIAL_EPOCH`)
     */
    public const int INITIAL_EPOCH = 0;

    /**
     * Share session epoch that closes the session (`ShareRequestMetadata.FINAL_EPOCH`)
     */
    public const int FINAL_EPOCH = -1;

    /**
     * `max_bytes` of a request that sets no limit
     */
    public const int DEFAULT_MAX_BYTES = 0x7fffffff;

    /**
     * `max.poll.records` of the Java share consumer, which it sends as `max_records` and `batch_size`
     */
    public const int DEFAULT_MAX_RECORDS = 500;

    /**
     * Topics to fetch, with the acknowledgements of their partitions
     *
     * @var list<ShareFetchRequestTopic>
     */
    protected readonly array $topics;

    /**
     * Topics whose partitions leave the share session
     *
     * @var list<ShareFetchRequestForgottenTopic>
     */
    protected readonly array $forgottenTopicsData;

    /**
     * @param string|null                           $groupId             Share group of the member
     * @param string|null                           $memberId            Member id of the member
     * @param int                                   $shareSessionEpoch   0 to open, -1 to close, else the next epoch
     * @param list<ShareFetchRequestTopic>          $topics              Topics to fetch and acknowledge
     * @param list<ShareFetchRequestForgottenTopic> $forgottenTopicsData Partitions to remove from the session
     */
    public function __construct(
        /**
         * Share group of the member
         */
        protected readonly ?string $groupId,
        /**
         * Member id of the member
         */
        protected readonly ?string $memberId,
        /**
         * Epoch of the share session
         */
        protected readonly int $shareSessionEpoch,
        array $topics = [],
        /**
         * Maximum time in milliseconds to wait for records
         */
        protected readonly int $maxWaitMs = 500,
        /**
         * Minimum bytes to accumulate in the answer
         */
        protected readonly int $minBytes = 1,
        /**
         * Maximum bytes of the answer
         */
        protected readonly int $maxBytes = self::DEFAULT_MAX_BYTES,
        /**
         * Maximum number of records to acquire, exceeded to finish a batch
         *
         * @since Version 1 of protocol
         */
        protected readonly int $maxRecords = self::DEFAULT_MAX_RECORDS,
        /**
         * Optimal number of records for the batches of acquired records and acknowledgements
         *
         * @since Version 1 of protocol
         */
        protected readonly int $batchSize = self::DEFAULT_MAX_RECORDS,
        array $forgottenTopicsData = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->topics              = array_values($topics);
        $this->forgottenTopicsData = array_values($forgottenTopicsData);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the topics of a request out of partitions and acknowledgements named by topic id
     *
     * Every partition of `$partitions` is fetched, and every partition of `$acknowledgements` is named with its
     * acknowledgement batches; a partition that is only acknowledged is named as well, which is how a member that
     * has what it needs acknowledges without fetching more of it. The order is that of the arguments.
     *
     * @param array<string, list<int>>                                   $partitions       Raw topic id => partitions
     * @param array<string, array<int, list<ShareAcknowledgementBatch>>> $acknowledgements Raw topic id => partition
     *        => acknowledgement batches
     *
     * @return list<ShareFetchRequestTopic>
     */
    public static function topicsOf(array $partitions, array $acknowledgements = []): array
    {
        $byTopic = [];
        foreach ($partitions as $topicId => $partitionIds) {
            foreach ($partitionIds as $partitionId) {
                $byTopic[(string) $topicId][$partitionId] = [];
            }
        }
        foreach ($acknowledgements as $topicId => $batchesByPartition) {
            foreach ($batchesByPartition as $partitionId => $batches) {
                $byTopic[(string) $topicId][$partitionId] = array_values($batches);
            }
        }

        $topics = [];
        foreach ($byTopic as $topicId => $partitionBatches) {
            $entries = [];
            foreach ($partitionBatches as $partitionId => $batches) {
                $entries[] = new ShareFetchRequestPartition((int) $partitionId, $batches);
            }
            $topics[] = new ShareFetchRequestTopic((string) $topicId, $entries);
        }

        return $topics;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return parent::getScheme() + [
            'groupId'             => BinarySchema::TYPE_NULLABLE_STRING,
            'memberId'            => BinarySchema::TYPE_NULLABLE_STRING,
            'shareSessionEpoch'   => BinarySchema::TYPE_INT32,
            'maxWaitMs'           => BinarySchema::TYPE_INT32,
            'minBytes'            => BinarySchema::TYPE_INT32,
            'maxBytes'            => BinarySchema::TYPE_INT32,
            'maxRecords'          => BinarySchema::TYPE_INT32,
            'batchSize'           => BinarySchema::TYPE_INT32,
            'topics'              => [ShareFetchRequestTopic::class],
            'forgottenTopicsData' => [ShareFetchRequestForgottenTopic::class],
        ];
    }

    /**
     * Returns the share session epoch of this request
     */
    public function getShareSessionEpoch(): int
    {
        return $this->shareSessionEpoch;
    }

    /**
     * Returns the topics of this request
     *
     * @return list<ShareFetchRequestTopic>
     */
    public function getTopics(): array
    {
        return $this->topics;
    }
}
