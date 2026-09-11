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
 * Produce response object, version 3
 *
 * <pre>
 *   ProduceResponse (Version: 3) => [TopicName [Partition ErrorCode Offset LogAppendTime]] ThrottleTime
 * </pre>
 *
 * `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2` in `ProduceResponse.schemaVersions()` @ 1.1.1 - the transactional
 * id that version 3 added lives in the REQUEST alone ({@see ProduceRequestV3}) - so this class decodes the very
 * same bytes as {@see ProduceResponseV2} and only states which request it was read back for.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v8)"
 */
final class ProduceResponseV3 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
