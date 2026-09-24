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
 * Produce response of version 11 (key 0)
 *
 * The answer of the abortable transaction error of KIP-890 (Kafka 3.8): the tagged `current_leader` of a partition
 * entry and the tagged `node_endpoints` of the body of version 10, exactly as {@see ProduceResponse} decodes them -
 * `ProduceResponse.json` @ 4.0.0 declares no field of version 12 either, and its comment is "Version 12 is the same
 * as version 10 (KIP-890)".
 *
 * What version 12 changes lives entirely in the request, and only for a transactional batch: see
 * {@see ProduceRequestV11}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The transaction protocol v2 of KIP-890
 *      part 2 (v12)"
 */
final class ProduceResponseV11 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 11;
}
