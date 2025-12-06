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

This project aims to have as few external dependencies as possible, relying on the Go standard library wherever practical. Third-party packages are generally not appreciated unless they provide a significant, unique benefit that is not feasible to implement directly.

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