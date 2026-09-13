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
 * ControlledShutdown response of the versions 0 to 2, the body of version 3 before KIP-482
 *
 * <pre>
 *   ControlledShutdown Response (Version: 0, 1 and 2) => error_code [remaining_partitions]
 * </pre>
 *
 * The frame did not change between the versions 0, 1 and 2 - `ControlledShutdownResponse.json` @ 2.8.2 says
 * "Versions 1 and 2 are the same as version 0" - so this one class reads all three. The version 3 of KIP-482 is the
 * same two fields in the flexible encoding.
 *
 * @see docs/protocol/2.8.md, section "ControlledShutdown API (key 7, v0 to v3)"
 */
final class ControlledShutdownResponseV2 extends ControlledShutdownResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
