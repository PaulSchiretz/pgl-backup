# Contributing

We'd love your help making `pgl-backup` even better! Whether it's reporting a bug, suggesting a new feature, or writing code, every contribution is appreciated.

## Found a Bug?

Oops! If you've run into a bug, we'd be grateful if you could open an issue. Please include as much detail as you can:
*   The version of `pgl-backup` you are using (`pgl-backup -version`).
*   Your operating system.
*   The steps to reproduce the bug.
*   Any relevant log output or error messages.

## Have a Great Idea?

We're always open to new ideas for making `pgl-backup` more useful. The best way to share an enhancement suggestion is to open an issue. This lets us discuss the idea and how it fits into the project.

## Our Philosophy on Dependencies

This project aims to have as few external dependencies as possible, relying on the Go standard library wherever practical. Third-party packages are used sparingly and only when they provide a significant, unique benefit that is not feasible to implement directly.

If you believe a new dependency is necessary, please open an issue to discuss it *before* submitting a pull request. This helps ensure that any additions align with the project's philosophy of simplicity and robustness.

## Ready to Contribute Code?

That's fantastic! Here’s how you can get your changes merged:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix (`git checkout -b feature/my-new-feature`).
3.  Make your changes.
4.  Ensure your code is well-formatted by running `go fmt ./...`.
5.  Run the test suite to ensure everything still passes: `go test ./...`.
6.  Commit your changes (`git commit -am 'Add some feature'`).
7.  Push to the branch (`git push origin feature/my-new-feature`).
8.  Create a new Pull Request.

## Architectural Overview

If you are diving into the code, here is a high-level overview of how the engine works.

### 1. Producer-Consumer Concurrency
To maximize I/O throughput, especially on high-latency network drives, the Sync, Compression, and Retention engines utilize a **Producer-Consumer** pattern.
*   **Producer ("Walker")**: A single goroutine scans the filesystem and feeds tasks (files to copy, backups to compress) into a buffered channel.
*   **Consumers ("Workers")**: A pool of goroutines reads from the channel and performs the blocking I/O operations in parallel.

### 2. Core Strategies
*   **Predictable Creation (Archiving)**: Archive creation is anchored to the *local system's midnight*. This ensures backups align with the user's calendar day. Crucially, it uses **Boundary Crossing** logic (e.g., "did we cross midnight?") rather than simple elapsed time. This ensures that even if you run backups every 10 minutes (updating the timestamp constantly), an archive is still correctly created once per day/week/etc.
*   **Consistent History (Retention)**: Retention policies apply standard calendar concepts (ISO weeks, YYYY-MM-DD) to the *UTC timestamp* stored in metadata. This ensures portable, consistent history.
*   **Fail-Forward (Compression)**: The compression engine only attempts to compress the specific backup created during the current run. If it fails (e.g., corrupt data), it leaves the backup uncompressed and moves on. It does *not* retry historical failures, preventing "poison pill" scenarios.