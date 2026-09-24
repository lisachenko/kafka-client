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

/**
 * UpdateFeatures answer of version 0 (Kafka 2.7, KIP-584): the answer of a request without the dry run
 *
 * The bytes are the ones of version 1 - `UpdateFeaturesResponse.json` @ 3.3.2 declares every field `0+` - because
 * KIP-778 changed the request alone ({@see UpdateFeaturesRequestV0}). What separates this answer from
 * {@see UpdateFeaturesResponseV1} is what it stands for: every result of this version is a change the controller
 * **wrote**, because a version 0 request has no `validate_only` to ask it not to.
 *
 * @see docs/protocol/4.3.md, sections "UpdateFeatures API (key 57, v0 to v2)" and "The upgrade type and the dry
 *      run of KIP-778 (v1)"
 */
final class UpdateFeaturesResponseV0 extends UpdateFeaturesResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
