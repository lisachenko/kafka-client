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

namespace Protocol\Kafka\Tests\Fixture;

use function getenv;
use function json_encode;

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\ClientQuotaAlteration;
use Protocol\Kafka\Admin\ClientQuotaAlterationOp;
use Protocol\Kafka\Admin\ClientQuotaEntity;
use Protocol\Kafka\Admin\ClientQuotaFilter;
use Protocol\Kafka\Admin\ClientQuotaFilterComponent;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use RuntimeException;

/**
 * Sets and removes the client quotas of the broker under test.
 *
 * Quotas are the only way to make a broker answer with a non-zero `ThrottleTime`. The lines up to 2.x wrote them
 * into ZooKeeper with the `kafka-configs.sh --zookeeper` tool of the distribution, which meant a `docker exec` into
 * the container of `docker-compose.yml`; since Kafka 2.6 the quotas have a protocol of their own (KIP-546, the apis
 * 48 and 49 that {@see AdminClient::alterClientQuotas()} speaks), and the 3.9.2 node of this line has no ZooKeeper
 * at all, so this fixture writes them through the wire. The broker applies an alteration before it answers it
 * (`ClientQuotaMetadataManager` on a KRaft node), so a quota is in force by the time {@see self::set()} returns.
 *
 * <code>
 *   $quota = ClientQuota::forClientId('my-test-client');
 *   $quota->set(['producer_byte_rate' => 1024]);
 *   try {
 *       // ... produce until the broker starts delaying the answers
 *   } finally {
 *       $quota->remove();
 *   }
 * </code>
 *
 * A quota that is left behind slows every later request of that client id down for good, so {@see self::set()}
 * must always be paired with a {@see self::remove()} in a `finally` block. The client id has to be unique for the
 * test that sets the quota: the broker of this suite is shared, and a quota is enforced for whoever sends that id.
 *
 * {@see self::isSupported()} tells whether a broker is configured at all (`KAFKA_BOOTSTRAP_SERVERS`), so that a
 * suite which runs without one skips those tests instead of failing them.
 *
 * @see docs/protocol/3.9.md, section "Quotas and throttle time"
 */
final class ClientQuota
{
    /**
     * Name of the environment variable that names the broker the quotas are written to
     */
    public const string BOOTSTRAP_ENV = 'KAFKA_BOOTSTRAP_SERVERS';

    /**
     * How long {@see self::set()} waits for the broker to report the quota it has just set, in seconds
     */
    private const int APPLY_TIMEOUT = 30;

    /**
     * Names of the quota entries that are currently set for the client id
     *
     * @var list<string>
     */
    private array $applied = [];

    private function __construct(
        private readonly string $clientId,
        private readonly AdminClient $admin
    ) {}

    /**
     * Builds the quota handle of a client id
     */
    public static function forClientId(string $clientId): self
    {
        $configuration = self::configuration();

        return new self($clientId, new AdminClient(Cluster::bootstrap($configuration), $configuration));
    }

    /**
     * Tells whether the quotas of the broker under test can be changed from here
     *
     * That needs a broker: the quota apis are spoken to whatever `KAFKA_BOOTSTRAP_SERVERS` names.
     */
    public static function isSupported(): bool
    {
        $servers = getenv(self::BOOTSTRAP_ENV);

        return $servers !== false && trim($servers) !== '';
    }

    /**
     * Sets the given quota entries for the client id, replacing whatever was set before
     *
     * @param array<string, int> $configuration `producer_byte_rate` and `consumer_byte_rate` in bytes per second
     */
    public function set(array $configuration): void
    {
        $ops = [];
        foreach ($configuration as $name => $value) {
            $ops[] = ClientQuotaAlterationOp::set($name, (float) $value);
        }

        $result = $this->admin->alterClientQuotas([new ClientQuotaAlteration($this->entity(), $ops)]);
        $error  = $result[(string) $this->entity()] ?? null;
        if ($error !== null) {
            throw new RuntimeException("Can not set the quota of {$this->clientId}: " . $error->getMessage(), 0, $error);
        }

        $this->applied = array_values(array_unique(array_merge($this->applied, array_keys($configuration))));
        $this->awaitApplied($configuration);
    }

    /**
     * Waits until the broker reports the quota entries it has just been asked to set
     *
     * A quota is a controller write that the broker enforces only once it has replayed the metadata record of it:
     * a request that follows the AlterClientQuotas answer at once reaches the broker before that, and is not
     * throttled. The lag is milliseconds on an idle node and seconds on a loaded CI runner; the DescribeClientQuotas
     * of the same broker reads the same replayed image, so a quota it reports is a quota the broker applies.
     *
     * @param array<string, int> $configuration The entries that were set
     */
    private function awaitApplied(array $configuration): void
    {
        $deadline = microtime(true) + self::APPLY_TIMEOUT;
        do {
            $stored = $this->admin->describeClientQuotas(ClientQuotaFilter::containsOnly([
                ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_CLIENT_ID, $this->clientId),
            ]))[(string) $this->entity()] ?? [];

            $missing = array_filter(
                $configuration,
                static fn(int $value, string $name): bool => ($stored[$name] ?? null) !== (float) $value,
                ARRAY_FILTER_USE_BOTH
            );
            if ($missing === []) {
                return;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        throw new RuntimeException(sprintf(
            'The broker did not apply the quota %s of %s within %d seconds, it reports %s',
            json_encode($configuration),
            $this->clientId,
            self::APPLY_TIMEOUT,
            json_encode($stored)
        ));
    }

    /**
     * Removes every quota entry this fixture has set, so that the shared broker is left as it was found
     *
     * The call is idempotent and never throws: it also runs on the failure path of a test, where an exception of
     * its own would hide the failure that got the test there.
     */
    public function remove(): void
    {
        if ($this->applied === []) {
            return;
        }

        try {
            $ops = array_map(static fn(string $name) => ClientQuotaAlterationOp::remove($name), $this->applied);
            $this->admin->alterClientQuotas([new ClientQuotaAlteration($this->entity(), $ops)]);
        } catch (\Throwable) {
            // The broker keeps whatever it has; the next test uses a client id of its own
        }

        $this->applied = [];
    }

    /**
     * Returns what the broker has stored for this client id, for the message of a failed assertion
     */
    public function describe(): string
    {
        try {
            $quotas = $this->admin->describeClientQuotas(ClientQuotaFilter::containsOnly([
                ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_CLIENT_ID, $this->clientId),
            ]));

            return (string) json_encode($quotas);
        } catch (\Throwable $e) {
            return 'the quotas of the broker can not be read: ' . $e->getMessage();
        }
    }

    private function entity(): ClientQuotaEntity
    {
        return ClientQuotaEntity::forClientId($this->clientId);
    }

    /**
     * @return array<string, mixed>
     */
    private static function configuration(): array
    {
        $servers = array_map(
            static fn(string $address): string => 'tcp://' . trim($address),
            explode(',', (string) getenv(self::BOOTSTRAP_ENV))
        );

        return [
            ClientConfig::BOOTSTRAP_SERVERS         => $servers,
            ClientConfig::CLIENT_ID                 => 'kafka-client-quota-fixture',
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ] + ClientConfig::getDefaultConfiguration();
    }
}
