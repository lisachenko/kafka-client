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

/**
 * DeleteTopics request of version 5, the flat topic-name array of every version below 6
 *
 * Kafka 2.8 replaced the `[]TopicNames` of the request by the `[]DeleteTopicState` of KIP-516 with the version 6 -
 * a structure that names a topic by its **name or by its id** - so the version 5 is the last one that sends names
 * alone. What the version 5 itself added is the `error_message` of the answer (Kafka 2.7).
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
final class DeleteTopicsRequestV5 extends DeleteTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
