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

use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * GroupCoordinator, version 6: asks any broker which broker coordinates a group or a transactional id (key 10)
 *
 * The offsets for a given consumer group are maintained by a specific broker called the group coordinator. i.e., a
 * consumer needs to issue its offset commit and fetch requests to this specific broker.
 *
 * It can discover the current coordinator by issuing a group coordinator request.
 *
 * The API is called ConsumerMetadataRequest in Kafka 0.8.2 (api key 10, v0, kafka/api/ConsumerMetadataRequest.scala);
 * it was renamed to GroupCoordinator in 0.9 without a change to the wire format, and Kafka 0.11 renamed it once
 * more, to **FindCoordinator**. The class keeps the name `GroupCoordinatorRequest` of the pre-schema `main` branch,
 * as every identifier of this repository does (rule 2 of `CLAUDE.md`); the 0.11 sources call the schemas
 * `FIND_COORDINATOR_REQUEST_V0` and `FIND_COORDINATOR_REQUEST_V1`.
 *
 * <pre>
 *   FindCoordinator Request (Version: 1 to 3) => coordinator_key coordinator_type
 *     coordinator_key  => STRING
 *     coordinator_type => INT8      -- since version 1
 *
 *   FindCoordinator Request (Version: 4 to 6) => coordinator_type [coordinator_keys]
 *     coordinator_type => INT8
 *     coordinator_keys => COMPACT_STRING      -- since version 4, in place of the single key
 * </pre>
 *
 * Version 1 (KIP-98, Kafka 0.11) is what made the api serve the transaction coordinator as well: the string that
 * version 0 called `group_id` became `coordinator_key`, and the trailing `coordinator_type` says what to look the
 * key up as - {@see self::COORDINATOR_TYPE_GROUP} for a consumer group, {@see self::COORDINATOR_TYPE_TRANSACTION}
 * for the transactional id of a producer. The property that carries the key keeps the name `consumerGroup` of the
 * pre-schema `main`, which knew no other coordinator type; with the transaction type it holds a transactional id.
 *
 * Version 2 (KIP-219, Kafka 2.0) added no field - `FindCoordinatorRequest.json` @ 2.8.2 has `Key` at `0+` and
 * `KeyType` at `1+` and nothing else below the flexible version 3 - so {@see GroupCoordinatorRequestV1} sends the
 * very same bytes one api version lower. What version 2 stands for is the throttling contract of KIP-219: a
 * throttled broker answers first and mutes the channel afterwards. A **coordinator type the broker does not know**
 * is answered by a 2.8.2 broker with the error code 42 (InvalidRequest) and the coordinator `-1:"":-1`, where a
 * 1.1.1 broker closed the connection.
 *
 * **Version 4 (KIP-699, Kafka 3.0) batched the api**: `FindCoordinatorRequest.json` @ 3.0.2 ends `Key` at the
 * version 3 and adds the array `CoordinatorKeys` at `4+`, so that one request can look up the coordinators of
 * several groups - or of several transactional ids - in one round trip. The `coordinator_type` stays a single
 * field in front of it and holds for **every** key of the batch, which is why a batch is always one type.
 * {@see self::forKeys()} builds such a request; the published constructor keeps naming one key and sends it as a
 * one-element batch, exactly as `FindCoordinatorRequest.Builder.build()` @ 3.9.2 turns a single key into one
 * (and a one-element batch back into a single key for the versions below 4). {@see GroupCoordinatorRequestV3} is
 * the same lookup with the single key of the versions below.
 *
 * **Version 5 (KIP-890, Kafka 3.8) added no field**: *"Version 5 adds support for new error code
 * TRANSACTION_ABORTABLE (KIP-890)"* (`FindCoordinatorRequest.json` @ 3.8.1), and the same sentence stands over
 * the answer. What the number buys is the promise that the client understands the error code **120**
 * {@see \Protocol\Kafka\Common\Errors\TransactionAbortableException}, which the transaction verification of
 * KIP-890 gives the producer apis; a coordinator lookup itself never produces it - a version 5 frame is the
 * version 4 frame with the number 5 in its header, and {@see GroupCoordinatorRequestV4} sends the one below.
 *
 * **Version 6 (KIP-932, Kafka 3.9) added no field either**: *"Version 6 adds support for share groups
 * (KIP-932)"* (`FindCoordinatorRequest.json` @ 3.9.2), over the request and over the answer alike. What the
 * number buys is a third coordinator *type*: `KafkaApis.getCoordinator` @ 3.9.2 refuses
 * {@see self::COORDINATOR_TYPE_SHARE} with the error code 42 (InvalidRequest) while `apiVersion < 6` and looks
 * it up from the version 6 on. Share groups are out of this line by the owner's decision, so the constant and
 * the version are all this client has of them; {@see GroupCoordinatorRequestV5} sends the version below.
 *
 * The two types are looked up in two different internal topics - `__consumer_offsets` for a group and
 * `__transaction_state` for a transactional id, `KafkaApis.handleFindCoordinatorRequest` @ 0.11.0.3 - and both
 * topics are created lazily by the first lookup that needs them, which is why that first request is answered with
 * the error code 15 (GroupCoordinatorNotAvailable) and the lookup has to be retried, see
 * {@see \Protocol\Kafka\Common\CoordinatorLookup}.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
class GroupCoordinatorRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::GROUP_COORDINATOR;

    /**
     * @inheritdoc
     */
    public const int VERSION = 6;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * The first version that looks several coordinators up at once (KIP-699, Kafka 3.0)
     *
     * `FindCoordinatorRequest.MIN_BATCHED_VERSION` @ 3.9.2 is the same number: below it the request names one
     * `key`, from it on it carries the array `coordinator_keys` instead.
     */
    public const int MIN_BATCHED_VERSION = 4;

    /**
     * Look the key up as a consumer group id, `CoordinatorType.GROUP` @ 0.11.0.3.
     *
     * This is the only lookup version 0 of the api can do, and the default of every version.
     *
     * @since Version 1 of protocol
     */
    public const int COORDINATOR_TYPE_GROUP = 0;

    /**
     * Look the key up as the transactional id of a producer, `CoordinatorType.TRANSACTION` @ 0.11.0.3.
     *
     * @since Version 1 of protocol
     */
    public const int COORDINATOR_TYPE_TRANSACTION = 1;

    /**
     * Look the key up as a share group id, `CoordinatorType.SHARE` @ 3.9.2 (KIP-932)
     *
     * Share groups are **out of this line by the owner's decision**: the client has no api that speaks them, and
     * this constant exists so that the type the version 6 of the api was added for can be named - and measured.
     * `KafkaApis.getCoordinator` @ 3.9.2 answers it with the error code 42 (InvalidRequest) below
     * {@see self::MIN_SHARE_VERSION}, with the code 31 (ClusterAuthorizationFailed) for a principal that has no
     * `CLUSTER_ACTION` on the cluster, and, on a node whose `group.coordinator.rebalance.protocols` does not
     * name `share`, with the code 15 (CoordinatorNotAvailable) from a `return` that carries the comment *"When
     * share coordinator support is implemented in KIP-932, a proper check will go here"*. The 15 is retriable
     * for {@see \Protocol\Kafka\Common\CoordinatorLookup}, so a lookup of this type on such a node retries until
     * its timeout runs out.
     *
     * @since Version 6 of protocol
     */
    public const int COORDINATOR_TYPE_SHARE = 2;

    /**
     * The first version that may ask for {@see self::COORDINATOR_TYPE_SHARE} (KIP-932, Kafka 3.9)
     *
     * Below it `KafkaApis.getCoordinator` @ 3.9.2 answers the type 2 with the error code 42 (InvalidRequest) and
     * the coordinator `-1:"":-1`, whatever the key is: the check is `keyType == CoordinatorType.SHARE.id &&
     * request.context.apiVersion < 6`.
     */
    public const int MIN_SHARE_VERSION = 6;

    /**
     * The keys of a batched lookup, all of them of {@see self::$coordinatorType}
     *
     * @since Version 4 of protocol
     *
     * @var list<string>
     */
    protected readonly array $coordinatorKeys;

    /**
     * @param string $consumerGroup   Group id, or the transactional id for a transaction lookup
     * @param int    $coordinatorType One of the `COORDINATOR_TYPE_*` constants, not on the wire before version 1
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     * @param list<string>|null $coordinatorKeys The batch of version 4; null - the default - looks the single
     *        `$consumerGroup` up and sends it as a one-element batch, see {@see self::forKeys()}
     */
    public function __construct(
        /**
         * The key to look the coordinator up for: a consumer group id, or a transactional id.
         *
         * The property name is the one of the pre-schema `main` branch, where the api could only look a group up;
         * the field is called `group_id` in version 0 of the api and `coordinator_key` from version 1 on, and it
         * is not on the wire of a version 4 frame at all, which carries the array instead.
         */
        protected readonly string $consumerGroup,
        /**
         * What to look the key up as, one of the `COORDINATOR_TYPE_*` constants.
         *
         * @since Version 1 of protocol
         */
        protected readonly int $coordinatorType = self::COORDINATOR_TYPE_GROUP,
        string $clientId = '',
        int $correlationId = 0,
        ?array $coordinatorKeys = null
    ) {
        $this->coordinatorKeys = $coordinatorKeys ?? [$consumerGroup];

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds the batched lookup of version 4 (KIP-699, Kafka 3.0): several keys of one type in one request
     *
     * Every key of the batch is looked up as {@see self::$coordinatorType}, and the answer carries one entry per
     * key with an error code of its own ({@see GroupCoordinatorResponse::coordinatorOf()}). An **empty** batch is
     * a legal request that names no key at all: a 3.9.2 node answers it with an empty `coordinators` array.
     *
     * The keys are not on the wire of a version below 4; such a frame carries {@see self::$consumerGroup}, which
     * this factory fills with the first key of the batch - the empty string for an empty one - so that a
     * keep-behind class of a lower version still sends a meaningful lookup.
     *
     * @param list<string> $coordinatorKeys Group ids, or transactional ids for a transaction lookup
     * @param int          $coordinatorType One of the `COORDINATOR_TYPE_*` constants, for every key of the batch
     * @param string       $clientId        A user specified identifier for the client making the request
     * @param int          $correlationId   A user-supplied value that the broker passes back unmodified
     *
     * @throws UnsupportedVersionException If a version below 4 is asked for more than one key, exactly as
     *         `FindCoordinatorRequest.Builder.build()` @ 3.9.2 refuses it with its `NoBatchedFindCoordinatorsException`
     */
    public static function forKeys(
        array $coordinatorKeys,
        int $coordinatorType = self::COORDINATOR_TYPE_GROUP,
        string $clientId = '',
        int $correlationId = 0
    ): static {
        if (static::VERSION < self::MIN_BATCHED_VERSION && count($coordinatorKeys) > 1) {
            throw new UnsupportedVersionException(
                [
                    'error' => sprintf(
                        'The version %d of the FindCoordinator api looks one key up per request, the '
                        . '`coordinator_keys` array arrived with the version %d in Kafka 3.0 (KIP-699)',
                        static::VERSION,
                        self::MIN_BATCHED_VERSION
                    ),
                    'keys' => implode(', ', $coordinatorKeys),
                ]
            );
        }

        return new static(
            $coordinatorKeys[0] ?? '',
            $coordinatorType,
            $clientId,
            $correlationId,
            array_values($coordinatorKeys)
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION < self::MIN_BATCHED_VERSION) {
            $body['consumerGroup'] = BinarySchema::TYPE_STRING;
        }
        if (static::VERSION >= 1) {
            $body['coordinatorType'] = BinarySchema::TYPE_INT8;
        }
        if (static::VERSION >= self::MIN_BATCHED_VERSION) {
            $body['coordinatorKeys'] = [BinarySchema::TYPE_STRING];
        }

        return $header + $body;
    }
}
