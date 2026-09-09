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
 * Asks the controller to move every leader off one broker, version 0: the header of this version has no client id.
 *
 * <pre>
 *   ControlledShutdownRequest (Version: 0) => BrokerId
 *     BrokerId => int32
 * </pre>
 *
 * This is the one request of the Kafka 0.8 protocol that cannot be sent with the common request header:
 * `ControlledShutdownRequest.readFrom` reads the client id only for `versionId > 0`, so a client id string on the
 * wire would be parsed as the broker id. The scheme that {@see ControlledShutdownRequest::getScheme()} builds drops
 * the field for this version, which makes the whole frame 12 bytes long.
 *
 * A 0.9.0.1 broker still serves this version; it is the only version a 0.8.2.2 broker understands.
 *
 * @see docs/protocol/0.10.2.md, section "ControlledShutdown API (key 7, v0 and v1)"
 */
final class ControlledShutdownRequestV0 extends ControlledShutdownRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param int $brokerId      Identifier of the broker that should be shut down
     * @param int $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(int $brokerId, int $correlationId = 0)
    {
        parent::__construct($brokerId, '', $correlationId);
    }
}
