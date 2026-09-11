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
 * CreateTopics answer of version 6, the answer of version 7 without the topic id of KIP-516
 *
 * Kafka 2.8 put the **`topic_id`** of the new topic into every result with the version 7 ("Version 7 returns the
 * topic ID of the newly created topic if creation is sucessful"); the versions 5 and 6 carry the name alone.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v7)"
 */
final class CreateTopicsResponseV6 extends CreateTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
