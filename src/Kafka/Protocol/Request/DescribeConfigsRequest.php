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
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;

/**
 * DescribeConfigs, version 0: reads the configuration of a topic or of a broker (ApiKey 32, Kafka 0.11, KIP-133)
 *
 * <pre>
 *   DescribeConfigs Request (Version: 0) => [resources]
 *     resources => resource_type resource_name [config_names]
 *       resource_type => INT8
 *       resource_name => STRING
 *       config_names  => NULLABLE_ARRAY of STRING
 * </pre>
 *
 * KIP-133 made the two things `kafka-configs.sh --describe` had to read out of ZooKeeper available through the
 * protocol. A **topic** resource is answered by any broker, because `AdminManager.describeConfigs` reads the
 * entity config of the topic from ZooKeeper and merges it with the log defaults of the broker that answers. A
 * **broker** resource can only be answered by that very broker: `AdminManager` compares the requested id with its
 * own `config.brokerId` and throws an `InvalidRequestException` - reported as the error code 42 of that resource -
 * for any other id, because the answer is the live `KafkaConfig` of the process.
 *
 * The whole request carries no timeout and no top-level anything: it is a plain array of resources, and every
 * resource of it gets an entry in the answer, with an error code of its own.
 *
 * @see docs/protocol/0.11.0.md, section "DescribeConfigs API (key 32, v0)"
 */
class DescribeConfigsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_CONFIGS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Resources to describe, in the order of the request
     *
     * The array is a LIST and not a map: a resource is identified by its type AND its name, so the topic `0` and
     * the broker `0` are two different resources that would collide in a map keyed by the name alone.
     *
     * @var list<DescribeConfigsRequestResource>
     */
    protected readonly array $resources;

    /**
     * @param list<DescribeConfigsRequestResource> $resources     Resources to describe
     * @param string                               $clientId      A user specified identifier for the client
     * @param int                                  $correlationId A user-supplied value the broker passes back
     */
    public function __construct(array $resources, string $clientId = '', int $correlationId = 0)
    {
        $this->resources = array_values($resources);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'resources' => [DescribeConfigsRequestResource::class],
        ];
    }
}
