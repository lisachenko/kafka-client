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
use Protocol\Kafka\Protocol\Data\ShareAcknowledgeRequestPartition;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgeRequestTopic;

/**
 * ShareAcknowledge, version 1: accepts, releases or rejects records a member acquired (ApiKey 79, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   ShareAcknowledge Request (Version: 1) => group_id member_id share_session_epoch [topics]
 *     group_id            => COMPACT_NULLABLE_STRING
 *     member_id           => COMPACT_NULLABLE_STRING
 *     share_session_epoch => INT32      -- never 0: the 123; -1 acknowledges and closes the share session
 *     topics              => topic_id [partitions]
 *       partitions => partition_index [acknowledgement_batches]
 * </pre>
 *
 * `ShareAcknowledgeRequest.json` @ 4.1.0 (`validVersions` `1`). The acknowledgements of a share fetch, without the
 * fetch: it travels in the share session of the member, whose epoch it advances as a ShareFetch does, and it cannot
 * open one - `SharePartitionManager.acknowledgeSessionUpdate` @ 4.3.1 answers the epoch 0 with the **123**. The epoch
 * **-1** acknowledges and then closes the session, releasing every record the member still holds.
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1)"
 */
class ShareAcknowledgeRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::SHARE_ACKNOWLEDGE;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * The api is flexible from its first version
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics to acknowledge records of
     *
     * @var list<ShareAcknowledgeRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param string|null                        $groupId           Share group of the member
     * @param string|null                        $memberId          Member id of the member
     * @param int                                $shareSessionEpoch Epoch of the share session, -1 to close it
     * @param list<ShareAcknowledgeRequestTopic> $topics            Acknowledgements, per topic and partition
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
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->topics = array_values($topics);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the topics of a request out of acknowledgements named by topic id
     *
     * @param array<string, array<int, list<ShareAcknowledgementBatch>>> $acknowledgements Raw topic id => partition
     *        => acknowledgement batches
     *
     * @return list<ShareAcknowledgeRequestTopic>
     */
    public static function topicsOf(array $acknowledgements): array
    {
        $topics = [];
        foreach ($acknowledgements as $topicId => $batchesByPartition) {
            $partitions = [];
            foreach ($batchesByPartition as $partitionId => $batches) {
                $partitions[] = new ShareAcknowledgeRequestPartition((int) $partitionId, array_values($batches));
            }
            $topics[] = new ShareAcknowledgeRequestTopic((string) $topicId, $partitions);
        }

        return $topics;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return parent::getScheme() + [
            'groupId'           => BinarySchema::TYPE_NULLABLE_STRING,
            'memberId'          => BinarySchema::TYPE_NULLABLE_STRING,
            'shareSessionEpoch' => BinarySchema::TYPE_INT32,
            'topics'            => [ShareAcknowledgeRequestTopic::class],
        ];
    }

    /**
     * Returns the share session epoch of this request
     */
    public function getShareSessionEpoch(): int
    {
        return $this->shareSessionEpoch;
    }
}
