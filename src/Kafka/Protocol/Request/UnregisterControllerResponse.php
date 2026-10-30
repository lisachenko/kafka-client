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

use Protocol\Kafka\Protocol\BinarySchema;

/**
 * UnregisterController response object, version 0 (key 94, Kafka 4.4, KIP-1312)
 *
 * <pre>
 *   UnregisterController Response (Version: 0) => throttle_time_ms error_code error_message TAG_BUFFER
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * The three fields of the UnregisterBroker answer: 0 once the `UnregisterControllerRecord` is committed, or the code
 * of the check that stopped the request with the message of its exception - the **136** `ControllerIdNotRegistered`
 * of an id the controller holds no registration for ("Controller ID 4242 is not currently registered."), the **31**
 * `ClusterAuthorizationFailed` of a principal without `ALTER` on the cluster, the **35** of a `metadata.version`
 * below 4.4-IV2.
 *
 * @see docs/protocol/overview.md, section "UnregisterController API (key 94, v0)"
 */
class UnregisterControllerResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the request, 0 when the registration was removed
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
