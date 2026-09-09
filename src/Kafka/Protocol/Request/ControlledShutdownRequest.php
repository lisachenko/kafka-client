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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Asks the controller of the cluster to move every leader and every replica off one broker, version 1.
 *
 * <pre>
 *   ControlledShutdownRequest (Version: 1) => BrokerId
 *     BrokerId => int32
 * </pre>
 *
 * Version 1, introduced by Kafka 0.9, is the version that finally uses the common request header of the protocol:
 * `ControlledShutdownRequest.readFrom` @ 0.10.2.2 reads the api version, the correlation id, then the client id
 * **only when `versionId > 0`**, and the broker id last. Version 0 - the only version a 0.8.2.2 broker speaks - has
 * no client id at all and lives in {@see ControlledShutdownRequestV0}; sending a client id with it would make the
 * broker read that string as the broker id.
 *
 * Only the active controller can serve this request, and a 0.9 Metadata response does not say which broker that is
 * (the `ControllerId` field arrived with Metadata v1 in Kafka 0.10), so a client has to try the brokers.
 *
 * A broker id that the controller does not know is answered with the error code **8 (BrokerNotAvailable)** on a
 * 0.10.2.2 broker, for both versions of the request: the controller throws `BrokerNotAvailableException` and
 * `ControlledShutdownRequest.handleError()` maps `e.getClass`. On 0.8.2.2 the very same situation produced the code
 * -1 (Unknown), because that release mapped `e.getCause`, which is null for a directly thrown exception. Verified
 * against the broker of this branch.
 *
 * @see docs/protocol/0.11.0.md, section "ControlledShutdown API (key 7, v0 and v1)"
 */
class ControlledShutdownRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::CONTROLLED_SHUTDOWN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Identifier of the broker that should be shut down
     */
    protected int $brokerId;

    /**
     * @param int    $brokerId      Identifier of the broker that should be shut down
     * @param string $clientId      A user specified identifier for the client making the request, ignored by v0
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(int $brokerId, string $clientId = '', int $correlationId = 0)
    {
        $this->brokerId = $brokerId;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        if (static::VERSION < 1) {
            // Version 0 does not read a client id for this api key, see the class docblock
            unset($header['clientId']);
        }

        return $header + [
            'brokerId' => BinarySchema::TYPE_INT32,
        ];
    }

    /**
     * Returns the identifier of the broker that this request asks to shut down
     */
    public function getBrokerId(): int
    {
        return $this->brokerId;
    }
}
