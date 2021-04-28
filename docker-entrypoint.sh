#!/usr/bin/env sh

set -e

# first args are a flags
if [ "$1" == "" ]; then
	set -- make bench
fi

exec "$@"
