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
 * The result of committing the offset of one partition inside a transaction, i.e. one entry of a `partitions` array
 *
 * <pre>
 *   TxnOffsetCommitResponsePartition => partition error_code
 *     partition  => INT32
 *     error_code => INT16
 * </pre>
 *
 * `TXN_OFFSET_COMMIT_PARTITION_ERROR_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3. Error codes of
 * `GroupCoordinator.handleTxnCommitOffsets`:
 *
 * | Code | Name                               | Meaning                                                          |
 * |------|------------------------------------|------------------------------------------------------------------|
 * | 0    | None                               | The offset is in `__consumer_offsets`, invisible until the commit |
 * | 14   | GroupLoadInProgress                | The coordinator is still reading the group out of the log         |
 * | 15   | GroupCoordinatorNotAvailable       | The group coordinator is not available on this broker             |
 * | 16   | NotCoordinatorForGroup             | Another broker coordinates this group                             |
 * | 47   | InvalidProducerEpoch               | The epoch is below the one the log holds for the producer id      |
 * | 48   | InvalidTxnState                    | The offsets partition is not part of an open transaction of this producer - the AddOffsetsToTxn is missing |
 * | 43   | UnsupportedForMessageFormat        | `__consumer_offsets` runs a `message.format.version` below 0.11.0 |
 * | 30   | GroupAuthorizationFailed           | The client may not `Read` the group                               |
 * | 12   | OffsetMetadataTooLarge             | The metadata of the offset is above `offset.metadata.max.bytes`   |
 *
 * @see docs/protocol/1.1.md, section "TxnOffsetCommit API (key 28, v0)"
 */
class TxnOffsetCommitResponsePartition implements BinarySchemaInterface
{
    /**
     * Id of the partition this entry belongs to
     */
    public int $partition;

    /**
     * Error code of that partition, 0 when the offset was written
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
