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

use Protocol\Kafka\Protocol\Data\ClientQuotaEntityData;

/**
 * The thing a client quota is attached to: a user, a client id, both of them together, or an address
 *
 * `ClientQuotaEntity` of the Java admin client. An entity is a **map of entity type to name**, because Kafka
 * attaches a quota to a combination as readily as to a single name, and a `null` name means the `<default>` entity
 * of that type - the quota that applies to everybody of that type who has none of their own.
 *
 * @see docs/protocol/2.8.md, section "DescribeClientQuotas API (key 48, v0)"
 */
final class ClientQuotaEntity
{
    /**
     * A quota attached to the `client.id` a connection announces
     */
    public const string TYPE_CLIENT_ID = ClientQuotaEntityData::TYPE_CLIENT_ID;

    /**
     * A quota attached to the authenticated principal of a connection
     */
    public const string TYPE_USER = ClientQuotaEntityData::TYPE_USER;

    /**
     * A quota attached to the address a connection comes from (KIP-612)
     */
    public const string TYPE_IP = ClientQuotaEntityData::TYPE_IP;

    /**
     * @param array<string, string|null> $entries Name of the entity per entity type, null for the default one
     */
    public function __construct(public readonly array $entries) {}

    /**
     * Builds the entity of one named client id
     */
    public static function forClientId(string $clientId): self
    {
        return new self([self::TYPE_CLIENT_ID => $clientId]);
    }

    /**
     * Builds the entity of one named user
     */
    public static function forUser(string $user): self
    {
        return new self([self::TYPE_USER => $user]);
    }

    /**
     * Builds the `<default>` entity of an entity type, i.e. the quota everybody without one of their own gets
     */
    public static function defaultOf(string $entityType): self
    {
        return new self([$entityType => null]);
    }

    /**
     * Returns the name this entity carries for an entity type, or null for the default entity and for a type it
     * does not name at all - {@see self::has()} tells the two apart
     */
    public function nameOf(string $entityType): ?string
    {
        return $this->entries[$entityType] ?? null;
    }

    /**
     * Returns whether this entity has a part of the given type
     */
    public function has(string $entityType): bool
    {
        return array_key_exists($entityType, $this->entries);
    }

    /**
     * Builds the entity out of the parts of an answer
     *
     * @param array<string, ClientQuotaEntityData> $parts Parts of the entity as the broker sent them
     */
    public static function fromData(array $parts): self
    {
        $entries = [];
        foreach ($parts as $part) {
            $entries[$part->entityType] = $part->entityName;
        }

        return new self($entries);
    }

    /**
     * Returns the parts of this entity in the shape the two apis put on the wire
     *
     * @return array<string, ClientQuotaEntityData>
     */
    public function toData(): array
    {
        $parts = [];
        foreach ($this->entries as $entityType => $entityName) {
            $parts[$entityType] = new ClientQuotaEntityData($entityType, $entityName);
        }

        return $parts;
    }

    /**
     * A stable, readable key of this entity, e.g. `client-id=t1-quota` or `user=<default>`
     */
    public function __toString(): string
    {
        $parts = [];
        foreach ($this->entries as $entityType => $entityName) {
            $parts[] = $entityType . '=' . ($entityName ?? '<default>');
        }
        sort($parts);

        return implode(',', $parts);
    }
}
