#!/usr/bin/env bash

# Test that the userauth tests fail, but don't panic.
# Pass "-libsqlite3" as an argument to the script to
# run the tests with the "libsqlite3" build tag.

set -euo pipefail

OUT="$(mktemp -t 'go-sqlite3.XXXXXX')"
trap 'rm "${OUT}"' EXIT

function test_sqlite_userauth() {
    local tags='sqlite_userauth'
    while (( $# > 0 )); do
        if [[ -n $1 ]]; then
            tags+=",$1"
        fi
        shift
    done

    echo "Running: \`go test -tags=${tags}\`"
    if go test -tags="${tags}" &> "${OUT}"; then
        cat "${OUT}"
        echo >&2 ''
        echo >&2 'FAIL: tests passed: expected them to fail'
        return 2
    fi

    if \grep -qF 'panic:' "${OUT}"; then
        cat "${OUT}"
        echo >&2 ''
        echo >&2 'FAIL: test panicked'
        return 2
    fi

    echo "PASS"
}

function main() {
    local tags=''
    if [[ ${1:-} == '-libsqlite3' ]]; then
        tags='libsqlite3'
    fi
    test_sqlite_userauth "${tags}"
}

main "$@"
