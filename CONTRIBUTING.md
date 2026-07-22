# Contributing to altertable-lakehouse-java

## Development Setup

1. Fork and clone the repository.
2. Install dependencies: `mvn dependency:resolve`.
3. Run tests: `mvn test`.

## Making Changes

1. Create a branch from `main`.
2. Add or update tests.
3. Run the full check suite: `mvn verify`.
4. Commit using [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, etc.).
5. Open a pull request.

## Code Style

This project uses Maven compiler lint checks and Maven formatting conventions. Run `mvn verify` before committing.

## Pull Requests

- Keep PRs focused on a single change.
- Update `CHANGELOG.md` under `[Unreleased]`.
- Ensure CI passes before requesting review.
