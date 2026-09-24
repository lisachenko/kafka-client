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
use Protocol\Kafka\Protocol\Data\AddRaftVoterRequestListener;

/**
 * AddRaftVoter, version 1: adds a voter to the metadata quorum (ApiKey 80, Kafka 3.9, KIP-853)
 *
 * <pre>
 *   AddRaftVoter Request (Version: 0 to 1) => cluster_id timeout_ms voter_id voter_directory_id [listeners]
 *                                             ack_when_committed
 *     cluster_id         => COMPACT_NULLABLE_STRING
 *     timeout_ms         => INT32
 *     voter_id           => INT32
 *     voter_directory_id => UUID
 *     listeners          => name host port
 *       name => COMPACT_STRING
 *       host => COMPACT_STRING
 *       port => UINT16
 *     ack_when_committed => BOOLEAN                  -- since version 1, "default": true
 * </pre>
 *
 * KIP-853 made the raft quorum of a KRaft cluster **reconfigurable**: a controller joins the set of voters while
 * the cluster runs, and leaves it again with {@see RemoveRaftVoterRequest}. A voter is a **key** of two parts, the
 * replica id and the directory id of the disk its metadata log lives on - a node that comes back with an empty disk
 * is a different voter - and the listeners are the endpoints the quorum reaches it at.
 *
 * `"listeners": ["controller", "broker"]` @ 4.0.0: a broker serves the api on its client listeners and forwards it
 * to the active controller, whose `KafkaRaftClient.handleAddVoterRequest` checks, in this order, the cluster id
 * (**104** `InconsistentClusterId` for a foreign one; null is accepted), the voter key (**42** for the zero
 * directory id) and the listener of the controller among the endpoints (**42** again), then hands the request to
 * `AddVoterHandler`: a voter change in flight (**7** `RequestTimedOut`), the `kraft.version` feature (**35** below
 * the level 1), a voter id the set already has (**126** `DuplicateVoter`), and only then an ApiVersions to the new
 * voter - whose answer must support the finalized `kraft.version` and whose log must have caught up before the
 * controller appends the new voter set and answers once it is committed.
 *
 * Kafka **4.0** made the feature generally available (a cluster formatted with `--standalone` or
 * `--initial-controllers` finalizes `kraft.version` 1) and gave the Java admin client `addRaftVoter()`, which
 * {@see \Protocol\Kafka\Admin\AdminClient::addRaftVoter()} is; the version 0 of the api is Kafka 3.9's and
 * `validVersions` @ 4.0.0 is still `0`.
 *
 * **Version 1 (Kafka 4.2) appended `ack_when_committed`**: "Version 1 adds the AckWhenCommitted field" stands above
 * the `validVersions` of `AddRaftVoterRequest.json` @ 4.2.0, a boolean with the `"default": "true"` of the version 0
 * behaviour - "When true, return a response after the new voter set is committed. Otherwise, return after the leader
 * writes the changes locally." `AddVoterHandler` @ 4.2.0 reads it only **after** the new voter answered its
 * ApiVersions and the leader appended the new voter set: false completes the answer right there, with the commit
 * still ahead, and every refusal before it - 104, 42, 35, 7 and 126 - is the same with either value. The flag is
 * what the controller's own auto-join of KIP-853 sends as false (`KafkaRaftClient.buildAddVoterRequest` @ 4.2.0); the
 * Java admin client @ 4.2.0 leaves it at its default. {@see AddRaftVoterRequestV0} is the frame below it.
 *
 * @see docs/protocol/4.3.md, sections "AddRaftVoter API (key 80, v0 and v1)" and "The acknowledgement mode of
 *      Kafka 4.2 (v1)"
 */
class AddRaftVoterRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ADD_RAFT_VOTER;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Endpoints of the new voter, indexed by the listener name
     *
     * @var array<string, AddRaftVoterRequestListener>
     */
    protected readonly array $listeners;

    /**
     * @param string|null                        $clusterId        Id of the cluster the voter joins, null to skip
     *        the check
     * @param int                                $timeoutMs        How long the controller may take
     * @param int                                $voterId          Replica id of the new voter
     * @param string                             $voterDirectoryId The 16 raw bytes of the directory id of the
     *        new voter
     * @param list<AddRaftVoterRequestListener>  $listeners        Endpoints the quorum reaches the new voter at
     * @param string                             $clientId         A user specified identifier for the client
     * @param int                                $correlationId    A value the broker passes back unmodified
     * @param bool                               $ackWhenCommitted True to be answered once the new voter set is
     *        committed, false once the leader has written it (version 1, Kafka 4.2)
     */
    public function __construct(
        protected readonly ?string $clusterId,
        protected readonly int $timeoutMs,
        protected readonly int $voterId,
        protected readonly string $voterDirectoryId,
        array $listeners,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Whether the controller answers once the new voter set is committed (true) or once it is written (false)
         *
         * @since Version 1 of protocol (Kafka 4.2)
         */
        protected readonly bool $ackWhenCommitted = true
    ) {
        $byName = [];
        foreach ($listeners as $listener) {
            $byName[$listener->name] = $listener;
        }
        $this->listeners = $byName;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'clusterId'        => BinarySchema::TYPE_NULLABLE_STRING,
            'timeoutMs'        => BinarySchema::TYPE_INT32,
            'voterId'          => BinarySchema::TYPE_INT32,
            'voterDirectoryId' => BinarySchema::TYPE_UUID,
            'listeners'        => ['name' => AddRaftVoterRequestListener::class],
        ];
        if (static::VERSION >= 1) {
            $body['ackWhenCommitted'] = BinarySchema::TYPE_BOOLEAN;
        }

        return $header + $body;
    }

    /**
     * Returns the id of the cluster the voter joins, null when the request does not name one
     */
    public function getClusterId(): ?string
    {
        return $this->clusterId;
    }

    /**
     * Returns how long the controller may take over this request
     */
    public function getTimeoutMs(): int
    {
        return $this->timeoutMs;
    }

    /**
     * Returns the replica id of the new voter
     */
    public function getVoterId(): int
    {
        return $this->voterId;
    }

    /**
     * Returns the 16 raw bytes of the directory id of the new voter
     */
    public function getVoterDirectoryId(): string
    {
        return $this->voterDirectoryId;
    }

    /**
     * Returns the endpoints of the new voter, indexed by the listener name
     *
     * @return array<string, AddRaftVoterRequestListener>
     */
    public function getListeners(): array
    {
        return $this->listeners;
    }

    /**
     * Returns whether the controller answers once the new voter set is committed (version 1, Kafka 4.2)
     */
    public function isAckWhenCommitted(): bool
    {
        return $this->ackWhenCommitted;
    }
}
