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
 * GroupCoordinator, version 1: asks any broker which broker coordinates a group or a transactional id (key 10)
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
 *   FindCoordinator Request (Version: 1) => coordinator_key coordinator_type
 *     coordinator_key  => STRING
 *     coordinator_type => INT8      -- since version 1
 * </pre>
 *
 * Version 1 (KIP-98, Kafka 0.11) is what made the api serve the transaction coordinator as well: the string that
 * version 0 called `group_id` became `coordinator_key`, and the trailing `coordinator_type` says what to look the
 * key up as - {@see self::COORDINATOR_TYPE_GROUP} for a consumer group, {@see self::COORDINATOR_TYPE_TRANSACTION}
 * for the transactional id of a producer. The property that carries the key keeps the name `consumerGroup` of the
 * pre-schema `main`, which knew no other coordinator type; with the transaction type it holds a transactional id.
 *
 * The two types are looked up in two different internal topics - `__consumer_offsets` for a group and
 * `__transaction_state` for a transactional id, `KafkaApis.handleFindCoordinatorRequest` @ 0.11.0.3 - and both
 * topics are created lazily by the first lookup that needs them, which is why that first request is answered with
 * the error code 15 (GroupCoordinatorNotAvailable) and the lookup has to be retried, see
 * {@see \Protocol\Kafka\Common\CoordinatorLookup}.
 *
 * @see docs/protocol/1.1.md, section "GroupCoordinator API (key 10, v0 and v1)"
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
    public const int VERSION = 1;

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
     * @param string $consumerGroup   Group id, or the transactional id for a transaction lookup
     * @param int    $coordinatorType One of the `COORDINATOR_TYPE_*` constants, not on the wire before version 1
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * The key to look the coordinator up for: a consumer group id, or a transactional id.
         *
         * The property name is the one of the pre-schema `main` branch, where the api could only look a group up;
         * the field is called `group_id` in version 0 of the api and `coordinator_key` from version 1 on.
         */
        protected readonly string $consumerGroup,
        /**
         * What to look the key up as, one of the `COORDINATOR_TYPE_*` constants.
         *
         * @since Version 1 of protocol
         */
        protected readonly int $coordinatorType = self::COORDINATOR_TYPE_GROUP,
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
        $body   = [
            'consumerGroup' => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 1) {
            $body['coordinatorType'] = BinarySchema::TYPE_INT8;
        }

        return $header + $body;
    }
}
