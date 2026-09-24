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
 * UpdateFeatures request of version 1 (Kafka 3.3, KIP-778): the upgrade type and the dry run, answered per feature
 *
 * The bytes of {@see UpdateFeaturesRequest} with the version 1 in the header: Kafka 4.0 raised the api to the
 * version 2 without a new field in the request, because the version 2 changes the **answer** alone - it carries no
 * per-feature `results` any more. A request of this version is answered with them
 * ({@see UpdateFeaturesResponseV1}) when the controller accepts the whole request; a refusal is the top-level error
 * of the answer at every version on a 4.x controller.
 *
 * @see docs/protocol/4.3.md, sections "The upgrade type and the dry run of KIP-778 (v1)" and "The answer without
 *      results (v2, Kafka 4.0)"
 */
final class UpdateFeaturesRequestV1 extends UpdateFeaturesRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
