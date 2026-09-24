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
 * ListClientMetricsResources answer of version 0 (Kafka 3.7, KIP-714): the names of the subscriptions
 *
 * Every entry is a {@see \Protocol\Kafka\Protocol\Data\ClientMetricsResourceV0}, a name without a type, and the
 * entries are keyed by that name, which is all an entry of this version has. Version 1 (Kafka 4.1, KIP-1142)
 * appended the type of every entry and made the answer a list ({@see ListClientMetricsResourcesResponse}).
 *
 * @see docs/protocol/4.3.md, sections "ListClientMetricsResources API (key 74, v0 and v1)" and "The config resources
 *      of KIP-1142 (v1)"
 */
final class ListClientMetricsResourcesResponseV0 extends ListClientMetricsResourcesResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
