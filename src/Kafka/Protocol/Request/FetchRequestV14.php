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

/**
 * Fetch request of version 14 (key 1)
 *
 * The first version of KIP-405 and the last one that carries the plain `replica_id` field in its body.
 * `FetchRequest.json` @ 3.5.2 declares no field of it - "Version 14 is the same as version 13 but it also receives
 * a new error called OffsetMovedToTieredStorageException (KIP-405)" - so this frame is the version 13 frame with
 * another number in its header, and what it states is that the client understands the error code **109**.
 *
 * Version 15 of the same release (KIP-903) deprecates the `replica_id` and puts the tagged `replica_state` in its
 * place, which is the only wire difference between the two, see {@see FetchRequest::$replicaState}.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)", "The tiered-storage error of KIP-405 (v14)"
 *      and "The replica state of KIP-903 (v15)"
 */
final class FetchRequestV14 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 14;
}
