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
 * DeleteTopics response of version 2 (Kafka 2.0), the frame of version 3 with a lower version field
 *
 * <pre>
 *   DeleteTopics Response (Version: 2) => throttle_time_ms [topic_error_codes]
 * </pre>
 *
 * `DELETE_TOPICS_RESPONSE_V3 = DELETE_TOPICS_RESPONSE_V2` in `Protocol.java` @ 2.1.1: Kafka 2.1 raised the api
 * without touching a byte, so this class only lowers the version constant that
 * {@see DeleteTopicsResponse::getScheme()} follows.
 *
 * What version 3 buys is the error code **73** `TOPIC_DELETION_DISABLED` of a cluster whose
 * `delete.topic.enable` is false; a request of this version is answered with the **42** `INVALID_REQUEST`
 * of the lines below instead (`KafkaApis.handleDeleteTopicsRequest` @ 2.8.2).
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v4)"
 */
final class DeleteTopicsResponseV2 extends DeleteTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
