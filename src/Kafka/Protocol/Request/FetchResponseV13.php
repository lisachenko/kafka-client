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
 * Fetch response of version 13 (key 1)
 *
 * The answer of {@see FetchRequestV13}, and the frame of every version above it: `FetchResponse.json` @ 3.5.2
 * declares no field for version 14 ("Version 14 is the same as version 13 but it also receives a new error called
 * OffsetMovedToTieredStorageException") and none for version 15 ("Version 15 is the same as version 14
 * (KIP-903)"), so the three versions of the answer differ in the number of their header alone. What differs is
 * which error codes a partition entry may carry, see {@see FetchResponse}.
 *
 * @see docs/protocol/3.9.md, sections "Fetch API (key 1, v0 to v15)" and "The tiered-storage error of KIP-405
 *      (v14)"
 */
final class FetchResponseV13 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 13;
}
