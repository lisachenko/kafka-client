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
 * OffsetFetchResponseTopic DTO, the versions 0 to 4 of the OffsetFetch API
 *
 * The topic entry never changed; this class only lowers the version constant that selects the partition class, so
 * that an answer below version 5 is read without the `committed_leader_epoch` of KIP-320.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
final class OffsetFetchResponseTopicV0 extends OffsetFetchResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
