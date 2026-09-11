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
use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;

/**
 * DescribeConfigs, version 1: reads the configuration of a topic or of a broker (ApiKey 32, Kafka 0.11, KIP-133)
 *
 * <pre>
 *   DescribeConfigs Request (Version: 1) => [resources] include_synonyms
 *     resources => resource_type resource_name [config_names]
 *       resource_type => INT8
 *       resource_name => STRING
 *       config_names  => NULLABLE_ARRAY of STRING
 *     include_synonyms => BOOLEAN
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
 * Kafka 1.1 (KIP-226) added the trailing `include_synonyms` boolean of this version. With the flag set, every entry
 * of the answer lists the places the broker looked for the value - `AdminManager.configSynonyms()` @ 1.1.1 - and
 * without it the synonym array of every entry is **empty**; nothing else about the answer changes.
 * {@see DescribeConfigsRequestV0} sends the version 0 frame of a 0.11 broker, whose answer has no synonyms and no
 * config source at all.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0 and v1)"
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
    public const int VERSION = 1;

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
     * @param bool                                 $includeSynonyms Ask for the synonyms of every option (version 1)
     * @param string                               $clientId      A user specified identifier for the client
     * @param int                                  $correlationId A user-supplied value the broker passes back
     */
    public function __construct(
        array $resources,
        /**
         * Whether every entry of the answer should list the places the broker read the value from
         *
         * @since Version 1 of protocol
         */
        protected readonly bool $includeSynonyms = false,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->resources = array_values($resources);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'resources' => [DescribeConfigsRequestResource::class],
        ];
        if (static::VERSION >= 1) {
            $body['includeSynonyms'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }
}
