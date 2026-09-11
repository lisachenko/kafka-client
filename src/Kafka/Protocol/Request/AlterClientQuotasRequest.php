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
use Protocol\Kafka\Protocol\Data\AlterClientQuotasRequestEntry;

/**
 * AlterClientQuotas, version 0: sets and removes client quotas (ApiKey 49, Kafka 2.6, KIP-546)
 *
 * <pre>
 *   AlterClientQuotas Request (Version: 0) => [entries] validate_only
 *     entries => entity [ops]
 *       entity => entity_type entity_name
 *       ops    => key value remove
 *     validate_only => BOOLEAN
 * </pre>
 *
 * The other half of KIP-546, and the end of `kafka-configs.sh --zookeeper` for quotas: a quota is now written with
 * a request instead of a znode. One **entry** carries one entity and every change asked for it, and every entry is
 * applied and answered on its own.
 *
 * `$validateOnly` asks the broker to check the request and change nothing - the answer has the same shape, so a
 * caller can tell a refusal from an acceptance without writing anything.
 *
 * **The version 0 is a plain frame**, like the one of its describing half: the flexible version 1 is Kafka 2.8.
 *
 * @see docs/protocol/2.8.md, section "AlterClientQuotas API (key 49, v0)"
 */
class AlterClientQuotasRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ALTER_CLIENT_QUOTAS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Entries of this request, in the order they were given
     *
     * @var list<AlterClientQuotasRequestEntry>
     */
    protected readonly array $entries;

    /**
     * @param list<AlterClientQuotasRequestEntry> $entries      Entities and the changes asked for them
     * @param bool                                $validateOnly Whether the broker only checks the request
     * @param string                              $clientId     A user specified identifier for the client
     * @param int                                 $correlationId A value the broker passes back unmodified
     */
    public function __construct(
        array $entries,
        protected readonly bool $validateOnly = false,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->entries = array_values($entries);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'entries'      => [AlterClientQuotasRequestEntry::class],
            'validateOnly' => BinarySchema::TYPE_BOOLEAN,
        ];
    }

    /**
     * Returns the entries of this request
     *
     * @return list<AlterClientQuotasRequestEntry>
     */
    public function getEntries(): array
    {
        return $this->entries;
    }

    /**
     * Returns whether the broker only validates the request instead of applying it
     */
    public function isValidateOnly(): bool
    {
        return $this->validateOnly;
    }
}
