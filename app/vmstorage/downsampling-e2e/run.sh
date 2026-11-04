#!/bin/sh
# Stock/fork switch test for time-based downsampling (apptest/tests/downsampling_test.go).
#
# Run from the repo root with the stock vmstorage binary of the same release as the fork base:
#
#   docker create --name vmds-stock victoriametrics/vmstorage:v1.155.0-cluster
#   docker cp vmds-stock:/vmstorage-prod bin/stock/vmstorage && docker rm vmds-stock
#   docker run --rm -v "$PWD:/src" -v vm-gomod:/go/pkg/mod -v vm-gocache:/root/.cache/go-build \
#     -w /src golang:1.27 sh app/vmstorage/downsampling-e2e/run.sh
#
# The test binary runs on the container filesystem, because the crash timing needs fast directory listings.
# It uses fixed ports 18400-18402 and 18420-18422 inside the container and runs the two tests one by one.
set -e
mkdir -p bin/fork
for a in vmstorage vminsert vmselect; do CGO_ENABLED=0 go build -o bin/fork/$a ./app/$a; done
export DOWNSAMPLING_E2E_FORK_VMSTORAGE=/src/bin/fork/vmstorage
export DOWNSAMPLING_E2E_STOCK_VMSTORAGE=/src/bin/stock/vmstorage
export APPTEST_STARTUP_TIMEOUT=30s
export VMINSERT_PATH=/src/bin/fork/vminsert VMSELECT_PATH=/src/bin/fork/vmselect
go test -c -o /tmp/dstests.test ./apptest/tests
mkdir -p /tmp/run && cd /tmp/run
/tmp/dstests.test -test.count=1 -test.v -test.parallel 1 -test.timeout 40m -test.run "${E2E_RUN:-TestClusterDownsamplingSwitch}"
