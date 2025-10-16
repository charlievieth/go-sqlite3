# Benchmark options
NO_TESTS = ^$
# BENCHMARKS = ^BenchmarkIndex('\$'|Hard|Torture|Periodic(Unicode)?)
BENCH_SUITE = BenchmarkSuite
BENCH_QUERY = BenchmarkSuite/.*BenchmarkQuery$
BENCH_EXEC_STEP = BenchmarkSuite/.*BenchmarkExecStep$

# TODO: join these into a single list
ALL_TEST_TAGS := sqlite_allow_uri_authority
ALL_TEST_TAGS += sqlite_app_armor
ALL_TEST_TAGS += sqlite_column_metadata
ALL_TEST_TAGS += sqlite_foreign_keys
ALL_TEST_TAGS += sqlite_fts5
ALL_TEST_TAGS += sqlite_icu
ALL_TEST_TAGS += sqlite_introspect
ALL_TEST_TAGS += sqlite_json
ALL_TEST_TAGS += sqlite_math_functions
ALL_TEST_TAGS += sqlite_preupdate_hook
ALL_TEST_TAGS += sqlite_secure_delete
ALL_TEST_TAGS += sqlite_see
ALL_TEST_TAGS += sqlite_stat4
ALL_TEST_TAGS += sqlite_trace
ALL_TEST_TAGS += sqlite_unlock_notify
ALL_TEST_TAGS += sqlite_vacuum_incr
ALL_TEST_TAGS += sqlite_vtable

space := $(subst ,, )
comma := ,

ALL_TEST_TAGS_JOINED = $(subst $(space),$(comma),$(ALL_TEST_TAGS))

# GO_VERSION = $(shell go version | grep -oE 'go[1-9]\.[0-9]+(\.[0-9]+)?')
# ifeq (,$(shell echo $(GO_VERSION_NUMBER) | grep -E 'go1\.2[2-9]'))
# 	export GOEXPERIMENT=cgocheck2
# endif

.PHONY: all
all: build

.PHONY: build
build:
	@go build -tags "libsqlite3"

.PHONY: test
test:
	@GOEXPERIMENT=cgocheck2 go test -tags "libsqlite3" -v

.PHONY: test_all
test_all:
	@GOEXPERIMENT=cgocheck2 go test -tags "$(ALL_TEST_TAGS_JOINED)" -v

# TODO: merge with the above target
.PHONY: test_all_libsqlite3
test_all_libsqlite3:
	GOEXPERIMENT=cgocheck2 go test -tags "$(ALL_TEST_TAGS_JOINED),libsqlite3" -v

.PHONY: qtest
qtest:
	@GOEXPERIMENT=cgocheck2 go test -tags "libsqlite3"

.PHONY: short
short:
	@go test -tags "libsqlite3" -short

.PHONY: testrace
testrace:
	@GOEXPERIMENT=cgocheck2 go test -tags "libsqlite3" -race

.PHONY: test_userauth_fails
test_userauth_fails:
	@./scripts/test-userauth-fails.bash

.PHONY: test_userauth_fails_libsqlite3
test_userauth_fails_libsqlite3:
	@./scripts/test-userauth-fails.bash -libsqlite3

.PHONY: testfull
testfull:
	@./scripts/test-full.bash -v

.PHONY: testfull_race
testfull_race:
	@./scripts/test-full.bash -v -race

.PHONY: bench
bench:
	@go test -tags "libsqlite3 darwin" -run $(NO_TESTS) -bench $(BENCH_SUITE) -benchmem

.PHONY: bench_stmt_rows
bench_stmt_rows:
	@go test -tags "libsqlite3 darwin" -run $(NO_TESTS) -bench $(BENCH_QUERY) -benchmem

.PHONY: bench_exec_step
bench_exec_step:
	@go test -tags "libsqlite3 darwin" -run $(NO_TESTS) -bench $(BENCH_EXEC_STEP) -benchmem

.PHONY: bench_mem
bench_mem:
	@go test -tags "libsqlite3 darwin" -run $(NO_TESTS) \
		-bench $(BENCH_SUITE) \
		-benchmem -memprofilerate 1 -memprofile mem.out -benchtime 5s

.PHONY: bench_cpu
bench_cpu:
	@go test -tags "libsqlite3 darwin" -run $(NO_TESTS) \
		-bench $(BENCH_SUITE) \
		-cpuprofile cpu.out -benchtime 5s

.PHONY: tags
tags:
	@\grep -ohP '(?<=//go:build )\w+(\s+\w+)?' *.go | sort -u
