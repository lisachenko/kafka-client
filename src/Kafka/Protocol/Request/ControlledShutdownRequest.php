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
 * Asks the controller of the cluster to move every leader and every replica off one broker.
 *
 * <pre>
 *   ControlledShutdownRequest => BrokerId
 *     BrokerId => int32
 * </pre>
 *
 * This request is the one exception to the common request header of the protocol: `ControlledShutdownRequest.readFrom`
 * of Kafka 0.8.2.2 reads the api version, the correlation id and the broker id, and **no client id at all**
 * (`core/src/main/scala/kafka/api/ControlledShutdownRequest.scala` @ 0.8.2.2). A client id string on the wire would
 * be parsed as the broker id, therefore the scheme of this request drops the `clientId` field of its parent. The
 * field came back with version 1 of the API in Kafka 0.9.
 *
 * Only the active controller can serve this request. A broker id that the controller does not know is answered with
 * the error code -1 (Unknown), which makes an unknown id a harmless probe of the api: the controller does throw
 * `BrokerNotAvailableException` (code 8) but `ControlledShutdownRequest.handleError()` maps `e.getCause` - which is
 * null for a directly thrown exception - so `ErrorMapping.codeFor(null)` falls back to the Unknown code. The broker
 * log shows what really happened ("Broker id 4242 does not exist."). Observed on Kafka 0.8.2.2.
 *
 * @see docs/protocol/0.9.0.md, section "ControlledShutdown API (key 7, v0)"
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
    public const int VERSION = 0;

    /**
     * Identifier of the broker that should be shut down
     */
    protected int $brokerId;

    /**
     * @param int $brokerId      Identifier of the broker that should be shut down
     * @param int $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(int $brokerId, int $correlationId = 0)
    {
        $this->brokerId = $brokerId;

        parent::__construct(self::API_KEY, '', $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        // 0.8.2.2 does not read a client id for this api key, see the class docblock
        unset($header['clientId']);

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
