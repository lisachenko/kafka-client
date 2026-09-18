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
 * One topic of a DescribeTopicPartitions request (key 75, Kafka 3.8, KIP-966)
 *
 * <pre>
 *   TopicRequest => Name
 *     Name => COMPACT_STRING
 *   </pre>
 *
 * `TopicRequest` of `DescribeTopicPartitionsRequest.json` @ 3.8.1 - a structure of a single field, which the
 * flexible encoding still gives a tagged-field section of its own, so an entry costs the name plus one byte.
 * A topic is named **by name only**: unlike Metadata v12 this api has no topic id in its request, because it is
 * the api of `kafka-topics.sh --describe` and of `Admin.describeTopics(Collection<String>)`.
 *
 * An **empty** topic array is not "no topic" here but **every topic of the cluster**
 * (`DescribeTopicPartitionsRequestHandler.handleDescribeTopicPartitionsRequest` @ 3.9.2 sets `fetchAllTopics`
 * from `request.topics().isEmpty()`), which is the opposite of what an empty array means in most other apis of
 * this protocol and the same as the null topic array of Metadata.
 *
 * @see docs/protocol/3.9.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
class DescribeTopicPartitionsRequestTopic implements BinarySchemaInterface
{
    /**
     * @param string $name Name of the topic to describe
     */
    public function __construct(
        /**
         * Name of the topic this entry asks for
         */
        public string $name = ''
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name' => BinarySchema::TYPE_STRING,
        ];
    }
}
