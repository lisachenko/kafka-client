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
use Protocol\Kafka\Protocol\Data\DescribeLogDirsRequestTopic;

/**
 * DescribeLogDirs, version 0: what each disk of one broker holds (ApiKey 35, Kafka 1.0, KIP-113)
 *
 * <pre>
 *   DescribeLogDirs Request (Version: 0) => [topics]
 *     topics => topic [partitions]     -- NULLABLE
 *       topic      => STRING
 *       partitions => INT32
 * </pre>
 *
 * KIP-113 gave a broker more than one data directory a client can reason about: `log.dirs` may name several disks,
 * every replica lives in exactly one of them, and this api is what says which. It is answered **by the broker whose
 * disks are asked about** and by no other one - `KafkaApis.handleDescribeLogDirsRequest` hands the partitions to the
 * local `ReplicaManager.describeLogDirs` - so an administrator asks each broker of the cluster separately, which is
 * what {@see \Protocol\Kafka\Admin\AdminClient::describeLogDirs()} does.
 *
 * The `topics` array is **nullable**, and the two empty shapes mean opposite things:
 *
 *  - `null` asks for **every replica of every directory** (`DescribeLogDirsRequest.isAllTopicPartitions`, which the
 *    Java admin client always sends). This is what `kafka-log-dirs.sh --describe` sends, and on a busy broker the
 *    answer is large - the container of this repository answers ~200 KB for a few thousand partitions;
 *  - an **empty array** asks for no replica at all, and the answer is the list of the directories with an empty
 *    topic array in each of them - 64 bytes on the two-directory container. It is the cheapest way to ask "which
 *    disks does this broker have, and are they online".
 *
 * A partition the broker does not host is not an error: the broker intersects the requested set with the logs it
 * has, so an unknown topic simply produces no entry anywhere in the answer.
 *
 * @see docs/protocol/2.8.md, section "DescribeLogDirs API (key 35, v0)"
 */
class DescribeLogDirsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_LOG_DIRS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Topics to describe indexed by the topic name, or null for every replica of every directory
     *
     * @var array<string, DescribeLogDirsRequestTopic>|null
     */
    protected readonly ?array $topics;

    /**
     * A value of the `$topicPartitions` map is either a list of partition ids or an already built topic DTO.
     *
     * @param array<string, list<int>|DescribeLogDirsRequestTopic>|null $topicPartitions Replicas to describe, as
     *        topic => list of partition ids; `null` asks for every replica of every log directory, an empty array
     *        for none of them
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(?array $topicPartitions = null, string $clientId = '', int $correlationId = 0)
    {
        if ($topicPartitions === null) {
            $this->topics = null;
        } else {
            $packedTopics = [];
            foreach ($topicPartitions as $topic => $partitions) {
                $packedTopics[$topic] = $partitions instanceof DescribeLogDirsRequestTopic
                    ? $partitions
                    : new DescribeLogDirsRequestTopic((string) $topic, $partitions);
            }
            $this->topics = $packedTopics;
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => [
                'topic'                     => DescribeLogDirsRequestTopic::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }
}
