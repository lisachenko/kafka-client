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
 * CreateTopics request of version 4 (Kafka 2.4, KIP-464), the body of version 5 before KIP-482
 *
 * <pre>
 *   CreateTopics Request (Version: 1 to 4) => [create_topic_requests] timeout validate_only
 * </pre>
 *
 * The version 4 is the one KIP-464 gave the meaning "-1 without an assignment means the broker defaults"; the
 * version 5 of the same release adds nothing to the request and only writes it with the compact types and the
 * tagged-field sections of KIP-482. This class is therefore what a broker below Kafka 2.4 - or one that does not
 * speak the flexible encoding of this api - is sent.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v6)"
 */
final class CreateTopicsRequestV4 extends CreateTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
