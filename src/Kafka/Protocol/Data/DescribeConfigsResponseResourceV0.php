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

/**
 * The configuration of one resource of a DescribeConfigs answer of the version 0
 *
 * The resource entry of a version 0 answer is the one of version 1, only its config entries carry an `is_default`
 * boolean instead of the `config_source` and the synonyms of KIP-226, so this class only lowers the version constant
 * that {@see DescribeConfigsResponseResource::entryClass()} follows.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v4)"
 */
final class DescribeConfigsResponseResourceV0 extends DescribeConfigsResponseResource
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
