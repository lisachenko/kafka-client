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
 * Fetch response of version 14 (key 1)
 *
 * The answer of {@see FetchRequestV14}: the version 13 frame, field for field, of a request that promised to
 * understand the **109** `OFFSET_MOVED_TO_TIERED_STORAGE` of KIP-405. Version 15 keeps it unchanged again
 * ("Version 15 is the same as version 14 (KIP-903)"), because everything that version added is in the request.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v17)", "The tiered-storage error of KIP-405 (v14)"
 *      and "The replica state of KIP-903 (v15)"
 */
final class FetchResponseV14 extends FetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 14;
}
