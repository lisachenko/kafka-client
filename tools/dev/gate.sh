#!/usr/bin/env bash
#
# The full quality gate of a worktree, as CLAUDE.md defines it: php -l on every PHP file, the code style, phpstan,
# the unit and compliance suites, and the whole integration suite against the broker of the branch on its four
# listeners. Runs everything with the JIT off (the tracing JIT of a PHP 8.5 CLI miscompiles the pure-PHP byte loops).
#
#   tools/dev/gate.sh [<worktree>] [unit|integration|all]
#
# The worktree defaults to the repository the script lives in; it is always resolved to an absolute path and the
# script never changes into its own directory, so a coordinator can gate any worktree from anywhere.
set -u

WORKTREE=$(cd "${1:-$(dirname "$0")/../..}" && pwd)
WHAT=${2:-all}
cd "$WORKTREE" || exit 1

export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
export KAFKA_SSL_BOOTSTRAP_SERVERS=${KAFKA_SSL_BOOTSTRAP_SERVERS:-127.0.0.1:9093}
export KAFKA_SASL_BOOTSTRAP_SERVERS=${KAFKA_SASL_BOOTSTRAP_SERVERS:-127.0.0.1:9094}
export KAFKA_SASL_SSL_BOOTSTRAP_SERVERS=${KAFKA_SASL_SSL_BOOTSTRAP_SERVERS:-127.0.0.1:9095}

status=0
step() {
    local name=$1; shift
    echo "==> $name"
    if "$@"; then
        echo "    ok: $name"
    else
        echo "    FAILED: $name"
        status=1
    fi
}

if [ "$WHAT" = unit ] || [ "$WHAT" = all ]; then
    step "php -l" sh -c "find src tests examples -name '*.php' -print0 | xargs -0 -n1 -P4 php -l | grep -v '^No syntax errors' || true"
    step "php-cs-fixer" vendor/bin/php-cs-fixer check
    step "phpstan" php vendor/bin/phpstan analyse --memory-limit=512M --no-progress
    step "phpunit unit + compliance" php -d opcache.jit=0 vendor/bin/phpunit --testsuite unit,compliance
fi
if [ "$WHAT" = integration ] || [ "$WHAT" = all ]; then
    if ! docker info >/dev/null 2>&1; then
        echo "    FAILED: the Docker daemon does not answer (nohup dockerd >/tmp/dockerd.log 2>&1 &)"
        status=1
    else
        step "phpunit integration (four listeners, zero skips expected)" \
            php -d opcache.jit=0 vendor/bin/phpunit --testsuite integration --display-skipped
    fi
fi
exit $status
