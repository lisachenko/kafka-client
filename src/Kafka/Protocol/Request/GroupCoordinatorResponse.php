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
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;
use Protocol\Kafka\Protocol\InlineStruct;

/**
 * GroupCoordinator response, version 2 (key 10, FindCoordinator in the sources since 0.11)
 *
 * Called ConsumerMetadataResponse in Kafka 0.8.2 (api key 10, v0); the version 0 layout below is unchanged in 0.9
 * and 0.10.
 *
 * <pre>
 *   FindCoordinator Response (Version: 1 and 2) => throttle_time_ms error_code error_message coordinator
 *     throttle_time_ms => INT32           -- since version 1
 *     error_code       => INT16
 *     error_message    => NULLABLE_STRING -- since version 1
 *     coordinator      => node_id host port
 *       node_id => INT32
 *       host    => STRING
 *       port    => INT32
 * </pre>
 *
 * Version 1 (KIP-98, Kafka 0.11) added the two fields that surround the error code: the `throttle_time_ms` that
 * KIP-124 gave fifteen apis of this release, and a human readable `error_message` that
 * `FindCoordinatorResponse.getErrorResponse()` @ 0.11.0.3 fills from `Errors.message()`. A 0.11.0.3 broker sends
 * the message of the error code and `null` for the code 0, so it is a description of the code and never a second
 * source of truth; a client acts on {@see self::$errorCode} alone.
 *
 * **A 2.8.2 broker fills the message of a successful lookup too**: `KafkaApis.handleFindCoordinatorRequest` @ 2.8.2
 * builds every answer with `Errors.message()`, and `Errors.NONE.message()` is the *name* of the constant, so a
 * lookup that succeeded carries the string `"NONE"` where a 0.11 or 1.1 broker sent `ff ff`. Version 2 (KIP-219,
 * Kafka 2.0) did not change the layout at all; {@see GroupCoordinatorResponseV1} decodes the same bytes.
 *
 * While the broker is still creating the internal topic the lookup needs - `__consumer_offsets` for a group,
 * `__transaction_state` for a transactional id - the answer is the error code 15 (GroupCoordinatorNotAvailable)
 * with the coordinator `-1:"":-1`, so the lookup is worth retrying.
 *
 * @see docs/protocol/2.8.md, section "GroupCoordinator API (key 10, v0 to v3)"
 */
class GroupCoordinatorResponse extends AbstractResponse
{
    /**
     * Version of the GroupCoordinator API that this class decodes the answer of
     */
    public const int VERSION = 3;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code.
     */
    public int $errorCode;

    /**
     * Human readable description of {@see self::$errorCode}, null when the lookup succeeded.
     *
     * @since Version 1 of protocol
     */
    public ?string $errorMessage = null;

    /**
     * Host and port information for the coordinator for a consumer group.
     */
    public GroupCoordinatorResponseMetadata $coordinator;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 1) {
            $body['throttleTimeMs'] = BinarySchema::TYPE_INT32;
        }
        $body['errorCode'] = BinarySchema::TYPE_INT16;
        if (static::VERSION >= 1) {
            $body['errorMessage'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        // `node_id`, `host` and `port` are three fields of the answer in `FindCoordinatorResponse.json`
        // @ 2.8.2, not a structure: the group of them must not get a tagged section of its own (KIP-482)
        $body['coordinator'] = new InlineStruct(GroupCoordinatorResponseMetadata::class);

        return $header + $body;
    }
}
