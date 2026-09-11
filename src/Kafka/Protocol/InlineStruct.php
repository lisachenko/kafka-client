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
 * Marks a DTO that groups fields of its parent instead of being a structure of the protocol
 *
 * The schemes of this client sometimes bundle fields that the Kafka specification writes **flat** into the parent
 * message: {@see \Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata} is the `node_id`, `host` and
 * `port` of a FindCoordinator answer, which `FindCoordinatorResponse.json` @ 2.8.2 lists as three ordinary fields
 * of the response and not as a nested structure. That grouping is a convenience of the object model and has no
 * shape of its own on the wire.
 *
 * The difference only becomes visible in a **flexible version** (KIP-482), where every real structure ends in a
 * tagged-field section: an inline group must not get one, because the broker does not write one for it. A class
 * that implements this interface therefore keeps the compact encoding of its parent for its fields and skips the
 * tagged section, in {@see BinarySchema::readObjectFromStream()}, {@see BinarySchema::writeObjectToStream()} and
 * {@see BinarySchema::getObjectTypeSize()} alike.
 *
 * @see docs/protocol/2.8.md, section "Flexible versions in the engine (KIP-482)"
 */
interface InlineStruct extends BinarySchemaInterface {}
