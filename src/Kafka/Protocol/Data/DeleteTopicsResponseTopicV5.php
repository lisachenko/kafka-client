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

/**
 * The result of deleting one topic in the version 5 of the DeleteTopics API
 *
 * <pre>
 *   DeleteTopicsResponseTopic (Version: 5) => Topic ErrorCode ErrorMessage
 * </pre>
 *
 * Kafka 2.8 inserted the **`topic_id`** of KIP-516 between the name and the error code with the version 6 and made
 * the name nullable; the version 5 is the frame with the error message of Kafka 2.7 and nothing else.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
final class DeleteTopicsResponseTopicV5 extends DeleteTopicsResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
