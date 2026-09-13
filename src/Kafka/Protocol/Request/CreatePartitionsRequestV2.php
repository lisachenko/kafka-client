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
 * CreatePartitions request of version 2, the frame of version 3 with a lower version field
 *
 * "Version 3 is identical to version 2 but may return a THROTTLING_QUOTA_EXCEEDED error"
 * (`CreatePartitionsRequest.json` @ 2.7.2): the version Kafka 2.7 added with KIP-599 changes no byte and only
 * promises that the client understands the error code **89** and retries after the `throttle_time_ms`.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 to v3)"
 */
final class CreatePartitionsRequestV2 extends CreatePartitionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
