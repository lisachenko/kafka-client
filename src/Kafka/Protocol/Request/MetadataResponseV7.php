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
 * Metadata response of version 7 (key 3)
 *
 * The answer of version 7 (Kafka 2.1, KIP-320): the throttle time, the brokers, the cluster id, the controller id
 * and the topics, whose partition entries carry the `leader_epoch` of that release. Version 8 (Kafka 2.3,
 * KIP-430) appended a `topic_authorized_operations` to every topic entry and a `cluster_authorized_operations` to
 * the end of the frame, see {@see MetadataResponse}.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v11)"
 */
final class MetadataResponseV7 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
