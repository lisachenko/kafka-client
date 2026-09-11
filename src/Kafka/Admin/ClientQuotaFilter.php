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

namespace Protocol\Kafka\Admin;

/**
 * The filter of {@see AdminClient::describeClientQuotas()}: a conjunction of components and the `strict` flag
 *
 * `ClientQuotaFilter` of the Java admin client. Every component names one entity type, and an entity has to match
 * all of them to be answered.
 *
 * **`strict` is about the types the filter does *not* name.** With `false` - what `containsOnly()` and the two
 * factories below build - an entity may carry parts of other types as well, so a filter for one `client-id` also
 * answers the quota that is attached to a `user` *and* that `client-id` together. With `true` the answer carries
 * only entities whose set of types is exactly the set the filter named.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0)"
 */
final class ClientQuotaFilter
{
    /**
     * @param list<ClientQuotaFilterComponent> $components Components every answered entity has to match
     * @param bool                             $strict     Whether entities of an unnamed type are excluded
     */
    private function __construct(public readonly array $components, public readonly bool $strict) {}

    /**
     * Matches every entity that satisfies the components, whatever else it carries
     *
     * @param list<ClientQuotaFilterComponent> $components
     */
    public static function contains(array $components): self
    {
        return new self(array_values($components), false);
    }

    /**
     * Matches only entities whose types are exactly the ones the components name
     *
     * @param list<ClientQuotaFilterComponent> $components
     */
    public static function containsOnly(array $components): self
    {
        return new self(array_values($components), true);
    }

    /**
     * Matches every quota entity of the cluster
     */
    public static function all(): self
    {
        return new self([], false);
    }

    /**
     * Returns the components in the shape the api puts on the wire
     *
     * @return list<\Protocol\Kafka\Protocol\Data\ClientQuotaComponentData>
     */
    public function toData(): array
    {
        return array_map(static fn(ClientQuotaFilterComponent $c) => $c->toData(), $this->components);
    }
}
