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
 * OffsetCommit request of version 3 (Kafka 0.11), the frame of version 4 with a lower version field
 *
 * <pre>
 *   OffsetCommit Request (Version: 3) => group_id generation_id member_id retention_time [topics]
 * </pre>
 *
 * Version 4 (KIP-219, Kafka 2.0) left the request untouched - the JSON specification of 2.8.2 gives the same
 * `versions` to every field of the versions 2 to 4 - so this class puts the very same bytes on the wire as
 * {@see OffsetCommitRequest} and differs in the version field of the header only. What the version number buys is
 * the throttling contract of KIP-219: a broker that throttles a version 4 request answers it **first** and mutes
 * the channel afterwards, where it used to mute the channel for the delay and answer after it.
 *
 * The answer of version 3 is the one of version 4 as well, so it is read with {@see OffsetCommitResponseV3}.
 *
 * @see docs/protocol/2.8.md, sections "OffsetCommit API (key 8, v0 to v7)" and "Quotas and throttle time"
 */
final class OffsetCommitRequestV3 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
