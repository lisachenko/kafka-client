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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One coordinator of a FindCoordinator answer of version 4 (Kafka 3.0, KIP-699)
 *
 * <pre>
 *   Coordinator => key node_id host port error_code error_message
 *     key           => COMPACT_STRING
 *     node_id       => INT32
 *     host          => COMPACT_STRING
 *     port          => INT32
 *     error_code    => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * **Version 4 (KIP-699) batched the api**: the request carries an array of `coordinator_keys` instead of one
 * `key`, and the answer carries an array of these entries instead of the four top-level fields
 * `error_code`, `error_message`, `node_id`, `host` and `port` - `FindCoordinatorResponse.json` @ 3.0.2 ends those
 * at the version 3. Every entry names the key it answers, so the order of the array does not matter and a batch
 * that asks for three keys is answered with one result per key, each with its own error code.
 *
 * The class is named after `FindCoordinatorResponseData.Coordinator` of the Java client @ 3.9.2, because the
 * structure is new in this release and has no name on the lines below; the message classes around it keep the
 * published names {@see \Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest} and
 * {@see \Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse} (rule 2 of `CLAUDE.md`).
 *
 * **The 3.9.2 node never fills the `error_message` of a version 4 answer**: `KafkaApis
 * .handleFindCoordinatorRequestV4AndAbove` @ 3.9.2 sets the key, the error code, the host, the node id and the
 * port of every entry and nothing else, so the field is the **empty string** the generated data class starts
 * with - for a lookup that succeeded as well as for one that failed. The one answer that does carry a message is
 * the one the *error path* builds, `FindCoordinatorRequest.getErrorResponse()`, which fills it from
 * `Errors.message()`. A client acts on {@see self::$errorCode} alone, as it does on every line below.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
class FindCoordinatorResponseCoordinator implements BinarySchemaInterface
{
    /**
     * The key this entry answers: a consumer group id, or a transactional id
     */
    public string $key;

    /**
     * The broker id of the coordinator, -1 when the lookup failed
     */
    public int $nodeId;

    /**
     * The hostname of the coordinator, empty when the lookup failed
     */
    public string $host;

    /**
     * The port on which the coordinator accepts requests, -1 when the lookup failed
     */
    public int $port;

    /**
     * Error of this key alone; the other keys of the same batch keep their own
     */
    public int $errorCode = KafkaException::NO_ERROR;

    /**
     * Human readable description of {@see self::$errorCode}, the empty string on a 3.9.2 node
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'key'          => BinarySchema::TYPE_STRING,
            'nodeId'       => BinarySchema::TYPE_INT32,
            'host'         => BinarySchema::TYPE_STRING,
            'port'         => BinarySchema::TYPE_INT32,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
