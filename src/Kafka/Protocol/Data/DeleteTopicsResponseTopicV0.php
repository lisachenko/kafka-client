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
 * The result of deleting one topic in the versions 0 to 4 of the DeleteTopics API
 *
 * <pre>
 *   DeleteTopicsResponseTopic (Version: 0 to 4) => Topic ErrorCode
 * </pre>
 *
 * Kafka 2.7 appended an `error_message` to the entry with the version 5 ("Version 5 adds ErrorMessage in the
 * response and may return a THROTTLING_QUOTA_EXCEEDED error", `DeleteTopicsResponse.json` @ 2.7.2); every version
 * below it ends after the error code, which is what this class only lowers the version constant for.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
final class DeleteTopicsResponseTopicV0 extends DeleteTopicsResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
