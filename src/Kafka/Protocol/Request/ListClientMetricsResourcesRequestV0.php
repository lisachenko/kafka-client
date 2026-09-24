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
 * ListClientMetricsResources request of version 0 (Kafka 3.7, KIP-714): the request without a body
 *
 * The frame is the request header v2 and the tag buffer of the body - `"fields": []` in
 * `ListClientMetricsResourcesRequest.json` @ 3.7.2 - one byte less than the single boolean of
 * {@see DescribeClusterRequestV0}. It asks for the names of the `client-metrics` configuration resources and
 * nothing else; version 1 (Kafka 4.1, KIP-1142) added the `resource_types` with which the api became
 * `ListConfigResources`, and the {@see ListClientMetricsResourcesRequest::$resourceTypes} of an instance of this
 * class never reaches the wire.
 *
 * @see docs/protocol/4.3.md, sections "ListClientMetricsResources API (key 74, v0 and v1)" and "The config resources
 *      of KIP-1142 (v1)"
 */
final class ListClientMetricsResourcesRequestV0 extends ListClientMetricsResourcesRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
