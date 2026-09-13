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
 * CreateTopics request of version 6, the frame of version 7 with a lower version field
 *
 * "Version 7 is the same as version 6" (`CreateTopicsRequest.json` @ 2.8.2): what Kafka 2.8 added with the version
 * 7 is in the **answer**, which carries the `topic_id` of the new topic (KIP-516). The request of the versions 5, 6
 * and 7 is one and the same frame.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v7)"
 */
final class CreateTopicsRequestV6 extends CreateTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
