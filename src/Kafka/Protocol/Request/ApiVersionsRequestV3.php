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
 * ApiVersions, version 3: the first flexible frame of the protocol (key 18, Kafka 2.4, KIP-511)
 *
 * <pre>
 *   ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER
 *     client_software_name    => COMPACT_STRING
 *     client_software_version => COMPACT_STRING
 * </pre>
 *
 * The frame of the version 4 that {@see ApiVersionsRequest} sends is this frame with a 4 in its header: Kafka 3.9
 * declares no field for the bump and only promises something about the *answer* (KAFKA-17011, see
 * {@see ApiVersionsResponse}). This class is therefore what a client sends to a broker of Kafka 2.4 to 3.8 - and
 * what every peer of the `2.x` line and below is asked with - and it is the class the version 3 wire vectors are
 * replayed through.
 *
 * @see docs/protocol/3.9.md, section "ApiVersions API (key 18, v0 to v4)"
 */
final class ApiVersionsRequestV3 extends ApiVersionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
