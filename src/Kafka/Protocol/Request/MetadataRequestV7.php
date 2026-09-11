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
 * Metadata request of version 7 (key 3)
 *
 * The frame of the versions 4 to 7: the nullable topic array and the `allow_auto_topic_creation` boolean, and
 * nothing else - `MetadataRequest.json` @ 2.8.2 has no field between version 4 and version 8. Version 7 (Kafka
 * 2.1, KIP-320) is what the answer gained, the `leader_epoch` of every partition; version 8 (Kafka 2.3, KIP-430)
 * appended the two booleans that ask for the authorized operations, see {@see MetadataRequest}.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v8)"
 */
final class MetadataRequestV7 extends MetadataRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
