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
 * ShareAcknowledge request of version 1 (Kafka 4.1, KIP-932): the frame without the renew flag
 *
 * The stable version of Kafka 4.1 - `ShareAcknowledgeRequest.json` @ 4.1.0, `validVersions` `1` - whose
 * acknowledgements know the types 0 to 3 only. Version 2 (Kafka 4.2, KIP-1222) added `is_renew_ack` behind the
 * epoch ({@see ShareAcknowledgeRequest}); the flag of the constructor is not written by this version.
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1 and v2)"
 */
final class ShareAcknowledgeRequestV1 extends ShareAcknowledgeRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
