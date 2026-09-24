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
 * Fetch request of version 13 (key 1)
 *
 * The version this client sent while the line spoke towards Kafka 3.1 to 3.4: the first one that names its topics
 * by **id** (KIP-516) and the last one before the two versions of Kafka 3.5. Version 14 (KIP-405) is this frame
 * byte for byte and adds one promise - that the client understands the error code **109**
 * `OFFSET_MOVED_TO_TIERED_STORAGE` - and version 15 (KIP-903) takes the top-level `replica_id` out of it, see
 * {@see FetchRequest}.
 *
 * A request of this version is therefore what a client sends that does not want to be told about an offset which
 * has moved to remote storage: a broker answers such a fetch **1** `OffsetOutOfRange` instead of the 109.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "The tiered-storage error of KIP-405
 *      (v14)"
 */
final class FetchRequestV13 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 13;
}
