#!/usr/bin/env bash

set -eo pipefail

# run coverage tests on the current Go package
function gocover() {
    local exit_code=0
    local tmpfile
    tmpfile="$(mktemp -t 'gocover')" || return 1
    go test -covermode=atomic -coverprofile="${tmpfile}" "$@" || {
        exit_code=1
    }
    go tool cover -html="${tmpfile}" || {
        rm "${tmpfile}"
        exit_code=1
    }
    return $exit_code
}

gocover "$@"
