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
 * ApiVersions, version 4: the KAFKA-17011 bump of Kafka 3.9 (key 18)
 *
 * <pre>
 *   ApiVersions Request (Version: 4) => client_software_name client_software_version TAG_BUFFER
 *     client_software_name    => COMPACT_STRING
 *     client_software_version => COMPACT_STRING
 * </pre>
 *
 * The frame of the version 3 with a 4 in its header: the version declares no field and only promises that the
 * client reads a supported feature whose `min_version` is 0 ({@see ApiVersionsResponse}). It ends where the version
 * 5 that {@see ApiVersionsRequest} sends goes on with the `cluster_id` and `node_id` of KIP-1242, so a broker
 * cannot check whom a version 4 request meant to reach and never answers it the 129: whatever the constructor is
 * given for the two fields stays out of the frame.
 *
 * It is what a client sends to a node of Kafka 3.9 to 4.3, and the class the version 4 wire vectors are replayed
 * through.
 *
 * @see docs/protocol/overview.md, section "ApiVersions API (key 18, v0 to v5)"
 */
final class ApiVersionsRequestV4 extends ApiVersionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
