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
 * The result of creating one topic, version 0 of the CreateTopics API
 *
 * <pre>
 *   CreateTopicsResponseTopicV0 => Topic ErrorCode
 *     Topic     => string
 *     ErrorCode => int16
 * </pre>
 *
 * `TOPIC_ERROR_CODE` in `Protocol.java` @ 0.10.2.2, the entry without the `ErrorMessage` that version 1 added; the
 * `errorMessage` property of the parent stays null for it. The class exists only to lower the version constant that
 * {@see CreateTopicsResponseTopic::getScheme()} follows.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v3)"
 */
final class CreateTopicsResponseTopicV0 extends CreateTopicsResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
