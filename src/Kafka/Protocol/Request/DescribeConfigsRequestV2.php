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
 * DescribeConfigs request of version 2 (Kafka 2.0), the frame of version 3 WITHOUT `include_documentation`
 *
 * <pre>
 *   DescribeConfigs Request (Version: 1 and 2) => [resources] include_synonyms
 * </pre>
 *
 * Kafka 2.4 gave the api no version at all and Kafka 2.6 added the **version 3** of KIP-569, whose request ends in
 * one more boolean. This class is the frame a broker below Kafka 2.6 understands; its `$includeDocumentation` is
 * ignored, because the byte has no place in it.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v4)"
 */
final class DescribeConfigsRequestV2 extends DescribeConfigsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
