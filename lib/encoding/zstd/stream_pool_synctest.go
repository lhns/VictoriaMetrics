//go:build !cgo && synctest

package zstd

// A decoder cannot be used outside the synctest bubble it was created in, so readers must not outlive a test.
const poolReaders = false
