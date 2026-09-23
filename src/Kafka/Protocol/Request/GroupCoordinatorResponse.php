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

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\FindCoordinatorResponseCoordinator;
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;
use Protocol\Kafka\Protocol\InlineStruct;
use UnexpectedValueException;

/**
 * GroupCoordinator response, version 6 (key 10, FindCoordinator in the sources since 0.11)
 *
 * Called ConsumerMetadataResponse in Kafka 0.8.2 (api key 10, v0); the version 0 layout below is unchanged in 0.9
 * and 0.10.
 *
 * <pre>
 *   FindCoordinator Response (Version: 1 to 3) => throttle_time_ms error_code error_message coordinator
 *     throttle_time_ms => INT32           -- since version 1
 *     error_code       => INT16
 *     error_message    => NULLABLE_STRING -- since version 1
 *     coordinator      => node_id host port
 *       node_id => INT32
 *       host    => STRING
 *       port    => INT32
 *
 *   FindCoordinator Response (Version: 4 to 6) => throttle_time_ms [coordinators]
 *     throttle_time_ms => INT32
 *     coordinators     => key node_id host port error_code error_message   -- since version 4
 * </pre>
 *
 * Version 1 (KIP-98, Kafka 0.11) added the two fields that surround the error code: the `throttle_time_ms` that
 * KIP-124 gave fifteen apis of this release, and a human readable `error_message` that
 * `FindCoordinatorResponse.getErrorResponse()` @ 0.11.0.3 fills from `Errors.message()`. A 0.11.0.3 broker sends
 * the message of the error code and `null` for the code 0, so it is a description of the code and never a second
 * source of truth; a client acts on the error code alone.
 *
 * **A 2.8.2 broker fills the message of a successful lookup too**: `KafkaApis.handleFindCoordinatorRequest` @ 2.8.2
 * builds every answer with `Errors.message()`, and `Errors.NONE.message()` is the *name* of the constant, so a
 * lookup that succeeded carries the string `"NONE"` where a 0.11 or 1.1 broker sent `ff ff`. Version 2 (KIP-219,
 * Kafka 2.0) did not change the layout at all; {@see GroupCoordinatorResponseV1} decodes the same bytes.
 *
 * **Version 4 (KIP-699, Kafka 3.0) replaced the five top-level fields with an array of
 * {@see FindCoordinatorResponseCoordinator}**, one entry per key of the batched request, each with the key it
 * answers and an error code of its own: `FindCoordinatorResponse.json` @ 3.0.2 ends `ErrorCode`, `ErrorMessage`,
 * `NodeId`, `Host` and `Port` at the version 3. Only the throttle time is left in front of the array.
 * {@see self::coordinatorOf()} reads one key out of either shape, and {@see GroupCoordinatorResponseV3} decodes
 * the answer of the versions below.
 *
 * **Version 5 (KIP-890, Kafka 3.8) changed no field either**: *"Version 5 adds support for new error code
 * TRANSACTION_ABORTABLE (KIP-890)"* (`FindCoordinatorResponse.json` @ 3.8.1). The number is the client's promise
 * to understand the code **120**, not a layout - a version 5 answer of this node is the version 4 answer with the
 * number 5 in the header of the request it belongs to, and {@see GroupCoordinatorResponseV4} decodes that one.
 *
 * **Version 6 (KIP-932, Kafka 3.9) changed no field either**: *"Version 6 adds support for share groups
 * (KIP-932)"* (`FindCoordinatorResponse.json` @ 3.9.2). It is the version that lets the coordinator type
 * {@see GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE} be asked for at all, and the answer of such a lookup
 * is an ordinary entry of the `coordinators` array with an error code of its own;
 * {@see GroupCoordinatorResponseV5} decodes the version below.
 *
 * While the broker is still creating the internal topic the lookup needs - `__consumer_offsets` for a group,
 * `__transaction_state` for a transactional id - the answer is the error code 15 (GroupCoordinatorNotAvailable)
 * with the coordinator `-1:"":-1`, so the lookup is worth retrying.
 *
 * @see docs/protocol/4.3.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
class GroupCoordinatorResponse extends AbstractResponse
{
    /**
     * Version of the GroupCoordinator API that this class decodes the answer of
     */
    public const int VERSION = 6;

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
     * Error code of the single lookup of the versions below 4, {@see KafkaException::NO_ERROR} above them.
     *
     * A version 4 answer has no top-level error code at all: every key of the batch carries its own, see
     * {@see self::$coordinators} and {@see self::coordinatorOf()}.
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Human readable description of {@see self::$errorCode}, null when the lookup succeeded.
     *
     * @since Version 1 of protocol
     */
    public ?string $errorMessage = null;

    /**
     * Host and port information for the coordinator for a consumer group.
     *
     * Only the versions below 4 carry it; a batched answer names one coordinator per key instead.
     */
    public GroupCoordinatorResponseMetadata $coordinator;

    /**
     * The result of every key of a batched lookup, indexed by that key
     *
     * @since Version 4 of protocol
     *
     * @var array<string, FindCoordinatorResponseCoordinator>
     */
    public array $coordinators = [];

    /**
     * Returns the result of one key, whatever version of the api this answer is
     *
     * A version 4 answer is asked for the entry of that key; anything below it names one coordinator at its top
     * level and the key is only used to fill {@see FindCoordinatorResponseCoordinator::$key} of the entry this
     * builds, so that a caller reads both shapes the same way.
     *
     * An answer that carries exactly **one** coordinator answers for whatever key was asked, even when that entry
     * names another one: `AbstractCoordinator.FindCoordinatorResponseHandler` @ 3.9.2 reads a single lookup the
     * same way - it takes `coordinators().get(0)` and never compares the key, and only refuses an answer that
     * carries more than one entry. A batch of several keys is looked up by key alone.
     *
     * @throws UnexpectedValueException If a batched answer carries no entry for the given key
     */
    public function coordinatorOf(string $key): FindCoordinatorResponseCoordinator
    {
        if (static::VERSION >= GroupCoordinatorRequest::MIN_BATCHED_VERSION) {
            if (isset($this->coordinators[$key])) {
                return $this->coordinators[$key];
            }
            if (count($this->coordinators) === 1) {
                return reset($this->coordinators);
            }

            throw new UnexpectedValueException(
                "The FindCoordinator answer carries no coordinator for the key '{$key}'"
            );
        }

        $result               = new FindCoordinatorResponseCoordinator();
        $result->key          = $key;
        $result->nodeId       = $this->coordinator->nodeId;
        $result->host         = $this->coordinator->host;
        $result->port         = $this->coordinator->port;
        $result->errorCode    = $this->errorCode;
        $result->errorMessage = $this->errorMessage;

        return $result;
    }

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
        if (static::VERSION >= GroupCoordinatorRequest::MIN_BATCHED_VERSION) {
            $body['coordinators'] = ['key' => FindCoordinatorResponseCoordinator::class];

            return $header + $body;
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
