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
 * The replica placement of ONE partition that a CreatePartitions request adds
 *
 * <pre>
 *   CreatePartitionsRequestAssignment => broker_ids
 *     broker_ids => ARRAY of INT32
 * </pre>
 *
 * `CreatePartitionsAssignment` of `CreatePartitionsRequest.json` @ 2.8.2, an entry of the topic's `assignments`
 * array. It is a **structure** with a single array field, not a bare array of broker ids, and in the plain encoding
 * of the versions 0 and 1 that difference is invisible - a structure is neither counted nor delimited on the wire,
 * so the frame is the very same bytes the flat `list<list<int>>` of this client used to write.
 *
 * From the **flexible** version 2 on it is visible and it costs a byte: every structure of a flexible frame ends in
 * a tagged-field section, so each assignment carries a trailing `00`. A frame without it is one byte short of what
 * the broker expects and the connection is **closed without an answer** - measured on the 2.8.2 container, which is
 * why this class exists. The counterpart of the same shape in CreateTopics is
 * {@see CreateTopicsRequestReplicaAssignment}, which always was a structure here because it carries the partition
 * id next to the replicas.
 *
 * @see docs/protocol/2.8.md, section "CreatePartitions API (key 37, v0 to v2)"
 */
class CreatePartitionsRequestAssignment implements BinarySchemaInterface
{
    /**
     * Broker ids that should host the added partition, the preferred leader first
     *
     * @var list<int>
     */
    public array $brokerIds;

    /**
     * @param list<int> $brokerIds Broker ids that should host the added partition
     */
    public function __construct(array $brokerIds = [])
    {
        $this->brokerIds = array_values($brokerIds);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return ['brokerIds' => [BinarySchema::TYPE_INT32]];
    }
}
