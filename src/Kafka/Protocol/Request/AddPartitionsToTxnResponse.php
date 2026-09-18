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
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResponseTopic;
use Protocol\Kafka\Protocol\Data\AddPartitionsToTxnResult;
use UnexpectedValueException;

/**
 * AddPartitionsToTxn response object, version 5 (key 24)
 *
 * <pre>
 *   AddPartitionsToTxn Response (Version: 0 to 3) => throttle_time_ms [errors]
 *     throttle_time_ms => INT32
 *     errors           => topic [partition_errors]
 *       topic            => STRING
 *       partition_errors => partition error_code
 *         partition  => INT32
 *         error_code => INT16
 *
 *   AddPartitionsToTxn Response (Version: 4)      => throttle_time_ms error_code [results_by_transaction]
 *     error_code             => INT16    -- since version 4, the first top-level code this api ever had
 *     results_by_transaction => transactional_id [topic_results]
 *       -- since version 4, one entry per transaction of the request
 * </pre>
 *
 * The api was born in Kafka 0.11, after KIP-124 made `throttle_time_ms` the first field of every new answer, so
 * there is no version of it without one.
 *
 * There is **no top-level error code**: everything the coordinator has to say - including the errors that are
 * really about the transactional id rather than about a partition, 47, 48, 49 and 51 - is repeated on every
 * partition of the answer, because `KafkaApis.handleAddPartitionToTxnRequest` @ 0.11.0.3 builds the answer by
 * mapping the single error of `handleAddPartitionsToTransaction` over the requested partitions. A client therefore
 * has to look at the partitions to learn what happened to the transaction, which is what
 * {@see \Protocol\Kafka\Client::addPartitionsToTxn()} does.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `ADD_PARTITIONS_TO_TXN_RESPONSE_V1 =
 * ADD_PARTITIONS_TO_TXN_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see AddPartitionsToTxnResponseV0} is the same frame with the version field of Kafka 0.11.
 *
 * **Version 4 (Kafka 3.5, KIP-890) answers one entry per transaction, behind a top-level error code.**
 * `AddPartitionsToTxnResponse.json` @ 3.5.2 - *"Version 4 adds support to batch multiple transactions and a top
 * level error code"* - ends the `errors` array at the version 3 and puts an `int16 error_code` and an array of
 * {@see AddPartitionsToTxnResult} in its place, each entry the topic array of one transactional id. The top-level
 * code is what refuses the **whole** frame: a 3.9.2 node answers a principal without `CLUSTER_ACTION` - every
 * client, because the version is the one brokers use - with the **31** (`ClusterAuthorizationFailed`) and an empty
 * `results_by_transaction`, and `AddPartitionsToTxnManager` @ 3.9.2 turns that 31 into the 48 a producer sees.
 * The per-partition codes of an answer that was served are the ones of a broker: a partition that is not in the
 * transaction is **120** (`TransactionAbortable`) and a stale epoch **90**, where the versions below 4 answer a
 * producer the 48 and the 47 (or the 90 from the version 2 on). {@see self::resultOf()} reads one transaction out
 * of either shape, and {@see AddPartitionsToTxnResponseV3} decodes the answer of the versions a client sends.
 *
 * **Kafka 3.8 added the version 5** (KIP-890), which declares no field either - *"Version 5 adds support for new
 * error code TRANSACTION_ABORTABLE"* (`AddPartitionsToTxnResponse.json` @ 3.8.1) - and is answered exactly like
 * the version 4 on a 3.9.2 node, the 120 of a `verify_only` included: the code belongs to the coordinator, not to
 * the version. {@see AddPartitionsToTxnResponseV4} keeps the frame of Kafka 3.5.
 *
 * @see docs/protocol/3.9.md, section "AddPartitionsToTxn API (key 24, v0 to v5)"
 */
class AddPartitionsToTxnResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;

    /**
     * The version 3 of Kafka 2.8 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the request as a whole, which the versions below 4 do not have at all
     *
     * @since Version 4 of protocol
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Result of every requested topic, indexed by the topic name
     *
     * Only the versions below 4 carry it at the top level; a version 4 answers the topics of each transaction of
     * its batch instead.
     *
     * @var array<string, AddPartitionsToTxnResponseTopic>
     */
    public array $errors = [];

    /**
     * The result of every transaction of a batched request, indexed by the transactional id
     *
     * @since Version 4 of protocol
     *
     * @var array<string, AddPartitionsToTxnResult>
     */
    public array $resultsByTransaction = [];

    /**
     * Returns the result of one transaction, whatever version of the api this answer is
     *
     * A version 4 answer is asked for the entry of that transactional id - the coordinator answers the
     * transactions of a batch in the order in which they finished, not in the order of the request, so an entry is
     * looked up and never taken by position; anything below it answers one transaction at its top level and the
     * given id only fills {@see AddPartitionsToTxnResult::$transactionalId} of the entry this builds.
     *
     * @throws UnexpectedValueException If a batched answer carries no entry for the given transactional id
     */
    public function resultOf(string $transactionalId): AddPartitionsToTxnResult
    {
        if (static::VERSION >= AddPartitionsToTxnRequest::MIN_BATCHED_VERSION) {
            return $this->resultsByTransaction[$transactionalId] ?? throw new UnexpectedValueException(
                "The AddPartitionsToTxn answer carries no entry for the transactional id '{$transactionalId}'"
            );
        }

        $result = new AddPartitionsToTxnResult();
        $result->transactionalId = $transactionalId;
        $result->topicResults    = $this->errors;

        return $result;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = ['throttleTimeMs' => BinarySchema::TYPE_INT32];

        if (static::VERSION >= AddPartitionsToTxnRequest::MIN_BATCHED_VERSION) {
            $body['errorCode']            = BinarySchema::TYPE_INT16;
            $body['resultsByTransaction'] = ['transactionalId' => AddPartitionsToTxnResult::class];

            return $header + $body;
        }

        $body['errors'] = ['topic' => AddPartitionsToTxnResponseTopic::class];

        return $header + $body;
    }
}
