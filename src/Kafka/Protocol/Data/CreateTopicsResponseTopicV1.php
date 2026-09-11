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
 * The result of creating one topic in the versions 1 to 4 of the CreateTopics API
 *
 * <pre>
 *   CreateTopicsResponseTopic (Version: 1 to 4) => Topic ErrorCode ErrorMessage
 * </pre>
 *
 * Kafka 2.4 appended the partition count, the replication factor and the configuration of the new topic to this
 * entry with the version 5 (KIP-525); everything below it ends after the error message, which is what this class
 * only lowers the version constant for.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v5)"
 */
final class CreateTopicsResponseTopicV1 extends CreateTopicsResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
