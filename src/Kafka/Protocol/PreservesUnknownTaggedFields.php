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
 * Keeps the tagged fields of a flexible structure that its scheme does not declare (KIP-482).
 *
 * A reader of a tagged-field section skips a tag it does not know by its announced size, which is what makes a
 * tagged field addable to a version that already exists. This trait is the second half of that contract: the
 * engine stores the raw value of every such tag here and writes it back untouched, so that a frame from a **newer**
 * broker survives a decode and encode round trip instead of losing bytes.
 *
 * Every request and response has it ({@see AbstractProtocolMessage}); a DTO that is decoded from a frame the
 * repository captures - anything a wire vector replays - should use it too. A structure without it drops what it
 * does not understand, which is what the Java client does with a message it re-serializes without its
 * `_unknownTaggedFields`.
 *
 * @see docs/protocol/2.8.md, section "Implementation model"
 */
trait PreservesUnknownTaggedFields
{
    /**
     * Raw value of every tagged field of the frame that the scheme of this class does not declare, by tag
     *
     * @var array<int, string>
     */
    protected array $unknownTaggedFields = [];

    /**
     * Returns the tagged fields this structure did not understand, as `tag => raw bytes`
     *
     * @return array<int, string>
     */
    public function getUnknownTaggedFields(): array
    {
        return $this->unknownTaggedFields;
    }
}
