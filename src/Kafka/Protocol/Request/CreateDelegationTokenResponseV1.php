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
 * The delegation token a KIP-219 request was answered with, version 1 (ApiKey 38)
 *
 * <pre>
 *   CreateDelegationToken Response (Version: 1) => error_code owner issue_timestamp expiry_timestamp
 *                                                 max_timestamp token_id hmac throttle_time_ms
 * </pre>
 *
 * The same eight fields as version 0 and version 2, with `throttle_time_ms` last in all three: the four token apis
 * of KIP-48 are the ones KIP-124 appended the field to instead of prepending it. Version 2 writes the very same
 * fields in the flexible encoding ({@see CreateDelegationTokenResponse}).
 *
 * @see docs/protocol/2.8.md, section "CreateDelegationToken API (key 38, v0 to v2)"
 */
final class CreateDelegationTokenResponseV1 extends CreateDelegationTokenResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
