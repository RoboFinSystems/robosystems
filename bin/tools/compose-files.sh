#!/bin/bash
set -e

# Print the `-f` arguments for the local Docker stack.
#
# compose.yaml runs every image as it was built. A service built from a local
# checkout also gets its source bind-mounted (compose.source*.yaml), so an edit
# applies on restart. A published image does not, so what runs is the release
# that was pulled.
#
# Usage: docker compose $(bin/tools/compose-files.sh [env-file]) ...

ENV_FILE=${1:-.env}

# The value compose resolves: the shell environment wins over the env file.
resolve() {
  if printenv "$1" >/dev/null; then
    printenv "$1"
  elif [ -f "$ENV_FILE" ]; then
    sed -n "s/^$1=//p" "$ENV_FILE" | tail -1
  fi
}

# A registry reference contains a slash (robofinsystems/robosystems:latest);
# a bare tag, or none, means the image is built here.
built_locally() {
  for NAME in "$@"; do
    case "$(resolve "$NAME")" in
    */*) return 1 ;;
    esac
  done
}

FILES="-f compose.yaml"

if built_locally ROBOSYSTEMS_IMAGE; then
  FILES="$FILES -f compose.source.yaml"
fi

if built_locally ROBOSYSTEMS_APP_IMAGE ROBOLEDGER_APP_IMAGE ROBOINVESTOR_APP_IMAGE; then
  FILES="$FILES -f compose.source.apps.yaml"
fi

echo "$FILES"
