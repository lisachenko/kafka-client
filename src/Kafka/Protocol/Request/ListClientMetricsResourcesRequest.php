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

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * ListClientMetricsResources, version 0: the names of the subscriptions (ApiKey 74, Kafka 3.7, KIP-714)
 *
 * <pre>
 *   ListClientMetricsResources Request (Version: 0) =>
 *     (no fields at all)
 * </pre>
 *
 * **The only request of this protocol with an empty body.** `ListClientMetricsResourcesRequest.json` @ 3.7.2
 * declares `"fields": []`, so the whole frame is the request header v2 and the tag buffer that every flexible
 * structure ends in - the question is the api key itself. DescribeCluster v0 used to be the smallest request
 * ({@see DescribeClusterRequestV0}) with its single boolean; this one is smaller.
 *
 * It answers the **names** of the `client-metrics` configuration resources of the cluster, i.e. the
 * subscriptions an operator wrote with `kafka-configs.sh --entity-type client-metrics`, which is what
 * `Admin.listClientMetricsResources()` of the Java client is for. What a named subscription *contains* - the
 * metric prefixes, the push interval, the client pattern - is read with DescribeConfigs (32) on the resource
 * type `CLIENT_METRICS` and is not part of this api.
 *
 * **This client never sends it**: the client-metrics apis are wire only on this line, by decision of the owner.
 *
 * @see docs/protocol/3.9.md, section "ListClientMetricsResources API (key 74, v0)"
 */
class ListClientMetricsResourcesRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::LIST_CLIENT_METRICS_RESOURCES;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * @param string $clientId      A user specified identifier for the client
     * @param int    $correlationId A value the broker passes back unmodified
     */
    public function __construct(string $clientId = '', int $correlationId = 0)
    {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     *
     * The header and nothing else: the specification gives this request no field of its own.
     */
    public static function getScheme(): array
    {
        return parent::getScheme();
    }
}
