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
 * OffsetFetch response object, version 1: the topics array and nothing else
 *
 * <pre>
 *   OffsetFetch Response (Version: 0 and 1) => [responses]
 *     responses => topic [partition_responses]
 * </pre>
 *
 * The group-level `error_code` that follows the topics array in version 2 does not exist here, so this class only
 * lowers the version constant that {@see OffsetFetchResponse::getScheme()} follows; reading a version 1 answer with
 * the version 2 class ({@see OffsetFetchResponseV2}) would read two bytes past the end of the frame. Every error of
 * these versions is reported per topic-partition, and {@see OffsetFetchResponse::$errorCode} stays 0.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v3)"
 */
final class OffsetFetchResponseV1 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
