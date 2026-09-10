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
 * It is the only version a 0.8.2.2 broker understands, and a 0.9.0.1 broker still serves it.
 *
 * A **0.9 to 0.11 broker reports `minVersion = 1` for this api** in its ApiVersions answer, so the version was
 * retired as far as the protocol was concerned - the Java client of those releases could not build the header any
 * more. The frame itself was still accepted, because key 7 was the last api such a broker parsed with its Scala
 * class: `RequestChannel.Request` called `ControlledShutdownRequest.readFrom()` for it before the version was
 * looked at at all, and that parser read a client id only for `versionId > 0` and never validated the version.
 *
 * **Kafka 1.0 made the version a first-class one again.** `RequestHeader` @ 1.1.1 carries a
 * `CONTROLLED_SHUTDOWN_V0_SCHEMA` - api key, api version and correlation id, no client id - for exactly this frame,
 * and `ControlledShutdownRequest.schemaVersions()` declares `{V0, V1}`, so a broker of this line announces
 * `minVersion = 0` and answers v0 through the ordinary schema path (and closes the connection on v2, which a 0.11
 * broker answered like v1). This class is therefore both the frame of the `0.8.x`/`0.9.x` vectors and a version a
 * 1.x broker serves; on this branch {@see ControlledShutdownRequest} (v1) is what the client sends.
 *
 * @see docs/protocol/1.1.md, section "ControlledShutdown API (key 7, v0 and v1)"
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
