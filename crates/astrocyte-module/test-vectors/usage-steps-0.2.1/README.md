# Usage step feed contract vectors

These files are copied byte for byte from `cortexkit-usage-steps` 0.2.1, contract
version 1, under its `test-vectors/usage-steps-v1/` directory. The published
archive has SHA-256 `af10d723b92f92221ce2e81d40e34a927b92775ed6fe97ccf63eb52ebc5deaf1`.
Its source is `cortexkit/commons`, commit
`7b1989fd5f89df8cc27ae7f3aa56d3311f4f415e`, under
`crates/cortexkit-usage-steps`. `SHA256SUMS` records the copied files' hashes.

The module's step-feed tests load the bytes with `include_bytes!`, so the tests
work without locating a machine's Cargo cache. The tests pass the read and
subscription requests through the real handlers, including both one-sided
filter refusals and the null-member read. They decode the served replies with
the pinned crate and check every row's counts.

The two standalone row vectors are seeded into the feed and read through the
handler. This tests the wire representation of a runner row without claiming
that production runner ingestion exists. The last-page and empty-session
reply vectors supply the expected fields. Only their opaque cursors are
adapted to astrocyte's cursor spelling; the test independently asserts the
known seeded cursors and global high-water mark.

The client-only malformed-row vectors are not provider operation requests.
The multi-page reader example contains superseded identities and is not an
exact expected page from astrocyte, whose read excludes superseded rows.
Those vectors are outside these provider-handler replay fixtures.
