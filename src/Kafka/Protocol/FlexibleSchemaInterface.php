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

namespace Protocol\Kafka\Protocol;

/**
 * A message or DTO that knows whether the version it stands for is a **flexible** one (KIP-482, Kafka 2.4).
 *
 * Flexibility is a property of an api *version*, not of a field: from the `flexibleVersions` of an api on, **every**
 * string, byte array and array of that version is written compactly and **every** structure of it ends in a
 * tagged-field section. There is no version in Kafka 2.8.2 that mixes the two encodings, which is why the engine
 * asks this question once per message and hands the answer down to every nested structure
 * ({@see BinarySchema::readObjectFromStream()}); a nested DTO therefore needs no flag of its own and the same
 * `FetchRequestTopic` scheme serves a v11 and a v12 Fetch request.
 *
 * @see docs/protocol/2.8.md, section "Implementation model"
 */
interface FlexibleSchemaInterface extends BinarySchemaInterface
{
    /**
     * Whether the version this class stands for is encoded with the compact types and tagged fields of KIP-482
     */
    public static function isFlexible(): bool;
}
