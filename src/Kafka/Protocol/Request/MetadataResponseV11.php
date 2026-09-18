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
 * Metadata response of version 11 (key 3)
 *
 * The answer KIP-700 (Kafka 2.8) created by dropping `cluster_authorized_operations` from the end of the version
 * 10 frame. It differs from the version 12 answer in one field only: the topic **name** of a topic entry is not
 * nullable here, because a request below version 12 can not name a topic by its id alone and the broker can
 * therefore never fail to resolve one, see {@see MetadataResponse} and
 * {@see \Protocol\Kafka\Common\TopicMetadataV10}.
 *
 * @see docs/protocol/3.9.md, sections "Metadata API (key 3, v0 to v12)" and "Metadata by topic id (v12, KIP-516)"
 */
final class MetadataResponseV11 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 11;
}
