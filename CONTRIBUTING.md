# Contributing to OptiSQL

Thank you for your interest in contributing to OptiSQL! This guide will help you get started.

## How to Write and Run Tests

We use a Makefile to simplify common development tasks. All commands should be run from the project root.

### Go Tests
- Run all tests
  ```bash
  make go-test
  ```
- Run tests with race detector
  ```bash
  make go-test-race
  ```
- Run tests with coverage
  ```bash
  make go-test-coverage
  ```

### Rust Tests
- Run all tests
  ```bash
  make rust-test
  ```

### Frontend Tests
- Run all tests
  ```bash
  make frontend-test
  ```

### Run All Tests (Go + Rust + Frontend)
- Run tests for both backends
  ```bash
  make test-all
  ```

## Pull Request (PR) Format

- Create a descriptive PR title that summarizes the change
- Include the following sections in your PR description:
  - **What**: Brief description of what this PR does
  - **Why**: Explanation of why this change is needed
  - **How**: Technical details of how the change was implemented
  - **Testing**: How you tested the changes
- Reference any related issues using `Fixes #<issue-number>` or `Closes #<issue-number>`
- Ensure all CI checks pass 

## Git Commit Message Format

- Use conventional commit format:
  ```bash
  <type>(<scope>): <subject>
  
  <body>
  
  <footer>
  ```
- **Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
- **Examples**:
  ```bash
  feat(operators): add hash join implementation
  
  fix(parser): handle edge case in substrait parsing
  
  docs(readme): update installation instructions
  
  test(filter): add unit tests for wildcard filter
  ```
- Keep subject line under 50 characters
- Capitalize the subject line
- Use imperative mood ("add" not "added")
- Separate subject from body with a blank line
- Wrap body at 72 characters

## Areas Where We Currently Need Help / Open Contributions

- **Operator Implementations**: Help implement missing SQL operators (join, filter, aggregation)
- **Substrait Integration**: Improve substrait plan parsing and optimization
- **Test Coverage**: Add unit tests and integration tests for existing functionality
- **Documentation**: Improve code comments, API documentation, and user guides
- **Performance Optimization**: Profile and optimize query execution performance
- **Bug Fixes**: Check the issues tab for open bugs that need fixing

## How to Build, Test, and Run the Application

### Prerequisites
- Go 1.24 or higher
- Rust 1.70 or higher
- Git
- Make

### Clone the Repository
```bash
git clone https://github.com/Rich-T-kid/OptiSQL.git
cd OptiSQL
```

### Quick Start with Makefile

See all available commands:
```bash
make help
```

### Build and Run Go Backend
```bash
make go-run
```

### Build and Run Rust Backend
```bash
make rust-run
```

### Build and Run Frontend
```bash
make frontend-run
```

### Run All Tests
```bash
make test-all
```

Or run individually:
```bash
make go-test
make rust-test
make frontend-test
```

### Run Linters
```bash
make lint-all
```

Or run individually:
```bash
make go-lint
make rust-lint
```

### Format Code
```bash
make fmt-all
```

### Clean Build Artifacts
```bash
make clean-all
```

### Verify CI Pipeline Locally
Before pushing, run the pre-push check:
```bash
make pre-push
```

This runs formatting, linting, and all tests to ensure your changes will pass CI.

---

For questions or help, please open an issue. 
