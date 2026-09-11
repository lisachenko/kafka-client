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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One filter component of a DescribeClientQuotas request (key 48, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   ComponentData => EntityType MatchType Match
 *     EntityType => STRING
 *     MatchType  => INT8   (0 exact name, 1 the default entity, 2 any name)
 *     Match      => NULLABLE_STRING
 * </pre>
 *
 * A component names **one** entity type and how its name is matched. `MATCH_TYPE_EXACT` takes the name in
 * `$match`, `MATCH_TYPE_DEFAULT` asks for the `<default>` entity of that type and ignores `$match`, and
 * `MATCH_TYPE_SPECIFIED` asks for every entity of the type that has a name of its own - the three values of
 * `ClientQuotaFilterComponent` @ 2.8.2. A filter with no component at all describes every quota of the cluster.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0)"
 */
class ClientQuotaComponentData implements BinarySchemaInterface
{
    /**
     * Match the entity whose name is exactly {@see self::$match}
     */
    public const int MATCH_TYPE_EXACT = 0;

    /**
     * Match the `<default>` entity of this type, whatever {@see self::$match} says
     */
    public const int MATCH_TYPE_DEFAULT = 1;

    /**
     * Match every entity of this type that carries a name of its own
     */
    public const int MATCH_TYPE_SPECIFIED = 2;

    /**
     * Entity type the component applies to, e.g. `client-id`, `user` or `ip`
     */
    public string $entityType;

    /**
     * How the name of the entity is matched, one of the MATCH_TYPE_* constants
     */
    public int $matchType;

    /**
     * Name to match, null for a match type that does not use one
     */
    public ?string $match;

    public function __construct(string $entityType, int $matchType, ?string $match = null)
    {
        $this->entityType = $entityType;
        $this->matchType  = $matchType;
        $this->match      = $match;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'entityType' => BinarySchema::TYPE_STRING,
            'matchType'  => BinarySchema::TYPE_INT8,
            'match'      => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
