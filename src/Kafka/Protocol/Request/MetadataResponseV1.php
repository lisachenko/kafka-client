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
 * Metadata response object, version 1 (key 3, Kafka 0.10.0)
 *
 * <pre>
 *   Metadata Response (Version: 1) => [brokers] controller_id [topic_metadata]
 * </pre>
 *
 * The answer of Kafka 0.10.0: the racks, the controller id and the internal flag of version 1 are all there, only
 * the `ClusterId` that Kafka 0.10.1 inserted between the brokers and the controller id is missing. Reading such an
 * answer with the version 2 class ({@see MetadataResponseV2}) would take the four bytes of the controller id for
 * the length of a cluster id string, so the class of the answer has to match the version of the request that asked
 * for it.
 *
 * @see docs/protocol/0.11.0.md, section "Metadata API (key 3, v0 to v4)"
 */
final class MetadataResponseV1 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
