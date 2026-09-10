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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of writing a control batch into one partition, i.e. one entry of a `partitions` array of an answer
 *
 * <pre>
 *   WriteTxnMarkersResponsePartition => partition error_code
 *     partition  => INT32
 *     error_code => INT16
 * </pre>
 *
 * `WRITE_TXN_MARKERS_PARTITION_ERROR_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3. Error codes of
 * `KafkaApis.handleWriteTxnMarkersRequest`:
 *
 * | Code | Name                          | Meaning                                                              |
 * |------|-------------------------------|----------------------------------------------------------------------|
 * | 0    | None                          | The control batch was appended to the partition                       |
 * | 3    | UnknownTopicOrPartition       | The broker does not host that partition                               |
 * | 6    | NotLeaderForPartition         | The broker is not the leader of it any more                           |
 * | 43   | UnsupportedForMessageFormat   | The `message.format.version` of the topic is below 0.11.0, so it has no place for a control batch |
 * | 52   | TransactionCoordinatorFenced  | The `coordinator_epoch` of the marker is below the one the leader saw |
 * | 31   | ClusterAuthorizationFailed    | The client may not perform a `ClusterAction`                          |
 *
 * @see docs/protocol/1.1.md, section "WriteTxnMarkers API (key 27, v0)"
 */
class WriteTxnMarkersResponsePartition implements BinarySchemaInterface
{
    /**
     * Id of the partition this entry belongs to
     */
    public int $partition;

    /**
     * Error code of that partition, 0 when the control batch was appended
     */
    public int $errorCode;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
