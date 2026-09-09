#!/usr/bin/env bash
# Installs the Composer dependencies WITHOUT downloading archives from GitHub.
#
# Some sandboxes (e.g. Claude Code remote sessions behind an egress proxy) block
# api.github.com / codeload.github.com, which makes a plain `composer install`
# fail with "Could not authenticate against github.com". Git clones of public
# repositories still work there, so this script:
#   1. installs every package from its git source (`--prefer-source`) into a
#      scratch copy of composer.json/composer.lock from which the two packages
#      that only ship as GitHub-release archives (phpstan/phpstan, rector/rector)
#      are removed,
#   2. fetches phpstan.phar from the phpstan/phpstan git repository at the locked
#      version and installs it as vendor/bin/phpstan,
#   3. strips the .git directories of the cloned packages (each vendor copy is
#      otherwise ~2.4 GB and fills the disk), and
#   4. moves the finished vendor/ tree next to this repository's composer.json.
#
# Usage:  tools/dev/vendor-from-source.sh [target-dir]   (default: the repo root)
# Reuse:  copy the produced vendor/ directory into other worktrees with `cp -a`;
#         the autoloader is relative, so it works in any checkout of this repo.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TARGET="${1:-$ROOT}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

export COMPOSER_ALLOW_SUPERUSER=1 COMPOSER_PROCESS_TIMEOUT=1800

cp "$ROOT/composer.json" "$ROOT/composer.lock" "$WORK/"
php -r '
    $dir = $argv[1];
    $drop = ["phpstan/phpstan", "rector/rector"];
    $json = json_decode(file_get_contents("$dir/composer.json"), true);
    foreach ($drop as $p) { unset($json["require-dev"][$p]); }
    file_put_contents("$dir/composer.json", json_encode($json, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
    $lock = json_decode(file_get_contents("$dir/composer.lock"), true);
    $lock["packages-dev"] = array_values(array_filter($lock["packages-dev"], fn($p) => !in_array($p["name"], $drop, true)));
    file_put_contents("$dir/composer.lock", json_encode($lock, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
' "$WORK"

PHPSTAN_VERSION="$(php -r 'foreach (json_decode(file_get_contents($argv[1]), true)["packages-dev"] as $p) if ($p["name"] === "phpstan/phpstan") echo $p["version"];' "$ROOT/composer.lock")"

(cd "$WORK" && composer install --prefer-source --no-progress --no-interaction)

git clone -q --depth 1 --branch "$PHPSTAN_VERSION" https://github.com/phpstan/phpstan "$WORK/phpstan-dist"
cp "$WORK/phpstan-dist/phpstan.phar" "$WORK/vendor/bin/phpstan"
chmod +x "$WORK/vendor/bin/phpstan"

find "$WORK/vendor" -type d -name .git -prune -exec rm -rf {} +

rm -rf "$TARGET/vendor"
mv "$WORK/vendor" "$TARGET/vendor"
echo "vendor/ installed into $TARGET (phpstan $PHPSTAN_VERSION as vendor/bin/phpstan)"
