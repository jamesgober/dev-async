# Changelog

## [Unreleased]

## [0.1.0] - 2026-05-07

### Added

- Initial crate skeleton.
- `run_with_timeout`: hard-timeout wrapper for a single future,
  produces a `dev-report::CheckResult`.
- `join_all_with_timeout`: collects verdicts for a vector of
  `JoinHandle`s with timeout enforcement.
- `AsyncCheck` trait for harness integration.
- Smoke tests covering pass, fail-by-timeout, and multi-task paths.

### Note

Name-claim release. Deadlock detection, task tracking, and shutdown
verification land in `0.2.x` and beyond.

[Unreleased]: https://github.com/jamesgober/dev-async/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jamesgober/dev-async/releases/tag/v0.1.0
