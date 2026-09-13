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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ClientQuotaComponentData;

/**
 * DescribeClientQuotas, version 1: reads the client quotas of the cluster (ApiKey 48, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   DescribeClientQuotas Request (Version: 0 and 1) => [components] strict
 *     components => entity_type match_type match
 *       entity_type => STRING
 *       match_type  => INT8
 *       match       => NULLABLE_STRING
 *     strict => BOOLEAN
 * </pre>
 *
 * KIP-546 gave the quotas of `kafka-configs.sh --entity-type clients` a protocol of its own: until Kafka 2.6 a
 * client quota could only be read and written through **ZooKeeper**, which is why every quota test of the lines
 * below this one shells into the container. The api is served by a ZooKeeper-backed broker
 * (`"listeners": ["zkBroker"]`) and answers from the quota cache of that broker, so any of them may be asked.
 *
 * **The filter is a conjunction of components**, one per entity type, and a request without a single component
 * describes every quota of the cluster. `$strict` decides what happens to an entity whose type the filter does
 * *not* name: `false` (the default) keeps it, so a filter for `client-id` also answers the quotas that are
 * attached to a `user` **and** a `client-id` together, while `true` answers only entities whose types are exactly
 * the ones the filter named.
 *
 * **The version 0 is a plain frame and the version 1 is the flexible one.** Kafka 2.6 added this api *after*
 * KIP-482 and still without the compact encoding - `DescribeClientQuotasRequest.json` @ 2.6.3 and @ 2.7.2 both say
 * `"flexibleVersions": "none"` - and only Kafka **2.8** adds the version 1, which is the same frame written
 * compactly and adds no field. {@see DescribeClientQuotasRequestV0} sends what a 2.6 or 2.7 broker serves.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0 and v1)"
 */
class DescribeClientQuotasRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_CLIENT_QUOTAS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 1;

    /**
     * Components of the filter, in the order they were given
     *
     * @var list<ClientQuotaComponentData>
     */
    protected readonly array $components;

    /**
     * @param list<ClientQuotaComponentData> $components    Filter components, an empty list describes everything
     * @param bool                           $strict        Whether entities of an unnamed type are excluded
     * @param string                         $clientId      A user specified identifier for the client
     * @param int                            $correlationId A value the broker passes back unmodified
     */
    public function __construct(
        array $components,
        protected readonly bool $strict = false,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->components = array_values($components);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'components' => [ClientQuotaComponentData::class],
            'strict'     => BinarySchema::TYPE_BOOLEAN,
        ];
    }

    /**
     * Returns the components of the filter
     *
     * @return list<ClientQuotaComponentData>
     */
    public function getComponents(): array
    {
        return $this->components;
    }

    /**
     * Returns whether entities of a type the filter does not name are excluded
     */
    public function isStrict(): bool
    {
        return $this->strict;
    }
}
