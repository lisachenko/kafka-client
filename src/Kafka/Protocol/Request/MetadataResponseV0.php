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
 * Metadata response object, version 0 (key 3)
 *
 * <pre>
 *   Metadata Response (Version: 0) => [brokers] [topic_metadata]
 * </pre>
 *
 * The answer every Kafka release from 0.8 on gives: bare `NodeId Host Port` brokers ({@see \Protocol\Kafka\Common\NodeV0})
 * and `TopicErrorCode TopicName [PartitionMetadata]` topics ({@see \Protocol\Kafka\Common\TopicMetadataV0}), with
 * neither a cluster id nor a controller id in between. The `clusterId` and `controllerId` properties of the parent
 * stay null for it.
 *
 * @see docs/protocol/0.10.2.md, section "Metadata API (key 3, v0, v1 and v2)"
 */
final class MetadataResponseV0 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
