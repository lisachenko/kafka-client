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

use Protocol\Kafka\Protocol\Data\DeleteTopicsResponseTopic;

/**
 * DeleteTopics response object, version 0 (key 20)
 *
 * <pre>
 *   DeleteTopics Response (Version: 0) => [topic_error_codes]
 *     topic_error_codes => topic error_code
 *       topic      => STRING
 *       error_code => INT16
 * </pre>
 *
 * One entry per topic of the request and nothing else. The error codes a 0.10.2.2 controller reports here are:
 *
 * | Code | Name                     | Meaning                                                                    |
 * |------|--------------------------|----------------------------------------------------------------------------|
 * | 0    | None                     | The topic was deleted within the `timeout` of the request                  |
 * | 3    | UnknownTopicOrPartition  | No topic of that name is in the metadata cache of the broker               |
 * | 7    | RequestTimedOut          | Marked for deletion, but not finished within `timeout` - a timeout of 0     |
 *                                    always gets it                                                             |
 * | 29   | TopicAuthorizationFailed | The client may describe the topic but not delete it                        |
 * | 41   | NotController            | The broker that was asked is not the active controller                     |
 *
 * @see docs/protocol/0.11.0.md, section "DeleteTopics API (key 20, v0)"
 */
class DeleteTopicsResponse extends AbstractResponse
{
    /**
     * Result of every topic of the request, indexed by the topic name
     *
     * @var array<string, DeleteTopicsResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => DeleteTopicsResponseTopic::class],
        ];
    }
}
