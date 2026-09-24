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
 * One group of an OffsetFetch answer of the versions 8 (Kafka 3.0) and 9 (Kafka 3.7): the entry that names its topics
 *
 * <pre>
 *   OffsetFetchResponseGroup => group_id [topics] error_code
 *     topics => topic [partition_responses]
 * </pre>
 *
 * The topics of the entry are indexed by their name, which `OffsetFetchResponse.json` @ 4.2.0 declares for the
 * versions `8-9`; version 10 (Kafka 4.2, KIP-848) names them by id in the {@see OffsetFetchResponseGroup} of that
 * version.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 */
final class OffsetFetchResponseGroupV8 extends OffsetFetchResponseGroup
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
