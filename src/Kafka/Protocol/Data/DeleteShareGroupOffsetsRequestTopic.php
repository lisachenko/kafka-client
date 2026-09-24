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
 * One topic of a DeleteShareGroupOffsets request (ApiKey 92, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DeleteShareGroupOffsetsRequestTopic => TopicName TAG_BUFFER
 *     TopicName => COMPACT_STRING
 * </pre>
 *
 * The api deletes the state of **whole topics**: there is no partition list.
 *
 * @see docs/protocol/4.3.md, section "DeleteShareGroupOffsets API (key 92, v0)"
 */
final class DeleteShareGroupOffsetsRequestTopic implements BinarySchemaInterface
{
    public function __construct(
        /**
         * Name of the topic whose share-group state is deleted
         */
        public string $topicName
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName' => BinarySchema::TYPE_STRING,
        ];
    }
}
