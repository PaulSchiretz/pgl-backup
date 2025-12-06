# Contributing

Contributions are welcome! Whether it's reporting a bug, suggesting a feature, or submitting a pull request, all contributions help make `pgl-backup` better.

## Reporting Bugs

If you find a bug, please open an issue on the project's GitHub page. Be sure to include:
*   The version of `pgl-backup` you are using (`pgl-backup -version`).
*   Your operating system.
*   The steps to reproduce the bug.
*   Any relevant log output or error messages.

## Suggesting Enhancements

If you have an idea for a new feature or an improvement to an existing one, feel free to open an issue to start a discussion.

## A Note on Dependencies

This project aims to have as few external dependencies as possible, relying on the Go standard library wherever practical. Third-party packages are generally not appreciated unless they provide a significant, unique benefit that is not feasible to implement directly.

If you believe a new dependency is necessary, please open an issue to discuss it *before* submitting a pull request. This helps ensure that any additions align with the project's philosophy of simplicity and robustness.

## Submitting Pull Requests

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix (`git checkout -b feature/my-new-feature`).
3.  Make your changes.
4.  Ensure your code is well-formatted by running `go fmt ./...`.
5.  Run the test suite to ensure everything still passes: `go test ./...`.
6.  Commit your changes (`git commit -am 'Add some feature'`).
7.  Push to the branch (`git push origin feature/my-new-feature`).
8.  Create a new Pull Request.