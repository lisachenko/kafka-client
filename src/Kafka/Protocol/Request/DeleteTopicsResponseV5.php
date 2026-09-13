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
 * DeleteTopics answer of version 5, the answer of version 6 without the topic id of KIP-516
 *
 * Kafka 2.8 put the **`topic_id`** of the deleted topic into every result with the version 6 and made the name
 * nullable with it; the version 5 carries the name, the error code and the error message it added itself.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
final class DeleteTopicsResponseV5 extends DeleteTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
