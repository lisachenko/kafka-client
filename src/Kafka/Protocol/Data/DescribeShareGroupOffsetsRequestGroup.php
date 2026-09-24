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
 * One share group of a DescribeShareGroupOffsets request (ApiKey 90, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DescribeShareGroupOffsetsRequestGroup => GroupId [Topics] TAG_BUFFER
 *     GroupId => COMPACT_STRING
 *     Topics  => COMPACT_NULLABLE_ARRAY of {@see DescribeShareGroupOffsetsRequestTopic}
 * </pre>
 *
 * **A null topic array asks for every topic-partition the group holds state for**; an empty one asks for none.
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0 and v1)"
 */
final class DescribeShareGroupOffsetsRequestGroup implements BinarySchemaInterface
{
    /**
     * Topics to describe, indexed by the topic name; `null` for every topic-partition of the group
     *
     * @var array<string, DescribeShareGroupOffsetsRequestTopic>|null
     */
    public ?array $topics;

    /**
     * @param string                          $groupId Id of the share group
     * @param array<string, list<int>>|null   $topics  Partitions to describe, as topic => partitions; `null` for
     *        every topic-partition the group holds state for
     */
    public function __construct(
        /**
         * Id of the share group
         */
        public string $groupId,
        ?array $topics = null
    ) {
        if ($topics === null) {
            $this->topics = null;

            return;
        }

        $packed = [];
        foreach ($topics as $topic => $partitions) {
            $packed[(string) $topic] = new DescribeShareGroupOffsetsRequestTopic((string) $topic, $partitions);
        }
        $this->topics = $packed;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId' => BinarySchema::TYPE_STRING,
            'topics'  => [
                'topicName'                 => DescribeShareGroupOffsetsRequestTopic::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }
}
