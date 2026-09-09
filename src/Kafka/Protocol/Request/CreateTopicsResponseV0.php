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
 * CreateTopics response object, version 0 (key 19)
 *
 * <pre>
 *   CreateTopics Response (Version: 0) => [topic_errors]
 *     topic_errors => topic error_code
 * </pre>
 *
 * The answer of a version 0 request carries the bare `topic error_code` entries of `TOPIC_ERROR_CODE` instead of the
 * `TOPIC_ERROR` of version 1, so this class only lowers the version constant that
 * {@see CreateTopicsResponse::topicClass()} follows. Reading a version 0 answer with the version 1 class would run
 * past the end of the frame while it looks for the `ErrorMessage`.
 *
 * @see docs/protocol/0.11.0.md, section "CreateTopics API (key 19, v0 and v1)"
 */
final class CreateTopicsResponseV0 extends CreateTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
