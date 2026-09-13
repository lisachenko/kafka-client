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
 * ControlledShutdown request of version 1 (Kafka 0.9), the frame of version 2 without the broker epoch
 *
 * <pre>
 *   ControlledShutdown Request (Version: 1) => broker_id
 *     broker_id => INT32
 * </pre>
 *
 * The version that added the ordinary request header - version 0 has none of its client id, see
 * {@see ControlledShutdownRequestV0} - and the last one whose body is the broker id alone: Kafka 2.2 appended the
 * `broker_epoch` int64 of KIP-380 to version 2, which is what {@see ControlledShutdownRequest} sends. This class
 * only lowers the version constant that its scheme follows.
 *
 * @see docs/protocol/2.8.md, section "ControlledShutdown API (key 7, v0 to v3)"
 */
final class ControlledShutdownRequestV1 extends ControlledShutdownRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
