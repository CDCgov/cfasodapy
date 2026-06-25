# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `Query.column_types` returns the dataset's column names and types

## 0.5.0 - 2026-06-15

### Added

- A changelog!

### Changed

- Use SODA version 3.0
- **Breaking**: App tokens are now required, as per SODA 3.0
- `Query` is by default an iterable, used like `for page in my_query`
- `Query.get_all()` is now a convience wrapper that accumulates pages

### Removed

- **Breaking**: Support for LIMIT and OFFSET clauses. (The API docs [do not recommend](https://dev.socrata.com/docs/queries/query) use of these clauses.)
