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
 * One resource of a DescribeConfigs answer of the versions 1 and 2, whose entries carry no type and no documentation
 *
 * The layout of the resource itself never changed; what the version decides is the class of its entries, which
 * {@see DescribeConfigsResponseResource::entryClass()} picks from the version constant this class lowers.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 to v3)"
 */
final class DescribeConfigsResponseResourceV1 extends DescribeConfigsResponseResource
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
