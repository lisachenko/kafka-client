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

/**
 * RemoveRaftVoter, version 0: removes a voter from the metadata quorum (ApiKey 81, Kafka 3.9, KIP-853)
 *
 * <pre>
 *   RemoveRaftVoter Request (Version: 0) => cluster_id voter_id voter_directory_id
 *     cluster_id         => COMPACT_NULLABLE_STRING
 *     voter_id           => INT32
 *     voter_directory_id => UUID
 * </pre>
 *
 * The other half of {@see AddRaftVoterRequest}: the voter is named by its whole **key**, the replica id and the
 * directory id, so a node that came back with a new disk is not removed by the key of the one it replaced.
 * `KafkaRaftClient.handleRemoveVoterRequest` @ 4.0.0 checks the cluster id (**104**, null accepted) and the voter
 * key (**42** for the zero directory id), and `RemoveVoterHandler` then a voter change in flight (**7**), the
 * `kraft.version` feature (**35** below the level 1) and whether the key is in the voter set at all (**127**
 * `VoterNotFound`); a known key is removed by appending the new voter set, and the answer waits for its commit. A
 * leader that removed **itself** resigns once the new set is committed - which is why this repository never sends
 * the key of the one voter of its node.
 *
 * There is no timeout in the request - the controller waits for the commit as long as its own
 * `controller.quorum.request.timeout.ms` - and no listener, because a voter that leaves needs no address.
 *
 * @see docs/protocol/4.3.md, section "RemoveRaftVoter API (key 81, v0)"
 */
class RemoveRaftVoterRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::REMOVE_RAFT_VOTER;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * @param string|null $clusterId        Id of the cluster the voter leaves, null to skip the check
     * @param int         $voterId          Replica id of the voter to remove
     * @param string      $voterDirectoryId The 16 raw bytes of the directory id of that voter
     * @param string      $clientId         A user specified identifier for the client
     * @param int         $correlationId    A value the broker passes back unmodified
     */
    public function __construct(
        protected readonly ?string $clusterId,
        protected readonly int $voterId,
        protected readonly string $voterDirectoryId,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'clusterId'        => BinarySchema::TYPE_NULLABLE_STRING,
            'voterId'          => BinarySchema::TYPE_INT32,
            'voterDirectoryId' => BinarySchema::TYPE_UUID,
        ];
    }

    /**
     * Returns the id of the cluster the voter leaves, null when the request does not name one
     */
    public function getClusterId(): ?string
    {
        return $this->clusterId;
    }

    /**
     * Returns the replica id of the voter to remove
     */
    public function getVoterId(): int
    {
        return $this->voterId;
    }

    /**
     * Returns the 16 raw bytes of the directory id of the voter to remove
     */
    public function getVoterDirectoryId(): string
    {
        return $this->voterDirectoryId;
    }
}
