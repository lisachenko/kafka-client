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
 * UnregisterController, version 0: removes the registration of a controller from the cluster metadata (ApiKey 94,
 * Kafka 4.4, KIP-1312)
 *
 * <pre>
 *   UnregisterController Request (Version: 0) => controller_id TAG_BUFFER
 *     controller_id => INT32
 * </pre>
 *
 * The controller counterpart of the UnregisterBroker api of KIP-500 (key 64): a controller that was removed from the
 * metadata quorum for good (RemoveRaftVoter, key 81) still has a `ControllerRegistrationRecord` in the metadata log,
 * and this request writes the `UnregisterControllerRecord` that removes it. `UnregisterControllerRequest.json` @
 * 4.4.0 declares the listeners `broker` and `controller`, so a broker forwards the request to the active controller
 * (`KafkaApis` @ 4.4.0, `forwardToController`), which authorizes `ALTER` on the `CLUSTER` resource
 * (`ControllerApis.handleUnregisterController`) and then refuses a `metadata.version` below 4.4-IV2 with the
 * **35** `UnsupportedVersion` and an id it holds no registration for with the **136** `ControllerIdNotRegistered`
 * of Kafka 4.4 (`ClusterControlManager.unregisterController` @ 4.4.0). There is **no other check**: the controller
 * does not ask whether the id is still a voter of the quorum, or even its own - which is why this repository never
 * sends the id of the one controller of its node. The api is flexible from its first version.
 *
 * @see docs/protocol/overview.md, section "UnregisterController API (key 94, v0)"
 */
class UnregisterControllerRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::UNREGISTER_CONTROLLER;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * @param int    $controllerId  Id of the controller whose registration is removed
     * @param string $clientId      A user specified identifier for the client
     * @param int    $correlationId A value the broker passes back unmodified
     */
    public function __construct(
        protected readonly int $controllerId,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'controllerId' => BinarySchema::TYPE_INT32,
        ];
    }

    /**
     * Returns the id of the controller whose registration is removed
     */
    public function getControllerId(): int
    {
        return $this->controllerId;
    }
}
