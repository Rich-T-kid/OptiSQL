# OptiSQL

A high-performance, in-memory query execution engine.

![Go Tests](https://github.com/Rich-T-kid/OptiSQL/actions/workflows/go-test.yml/badge.svg)
![Rust Tests](https://github.com/Rich-T-kid/OptiSQL/actions/workflows/rust-test.yml/badge.svg)
![Frontend Tests](https://github.com/Rich-T-kid/OptiSQL/actions/workflows/frontend-test.yml/badge.svg)


## Overview

OptiSQL is a custom in-memory query execution engine. The backend (physical execution) is built using golang and rust.The front end (query parsing & optimization) is built using C++.

**Technologies:**
- Go/Rust (physical optimizer, operators)
- Substrait (logical/physical plan representation)
- C++ (query parser & optimizer)
- ect (make,git,s3)
## Getting Started

### Prerequisites
- Go 1.24+
- Rust 1.70+
- C++23
- Python 3.11+
- Docker 29+
- Make
- git

### Quick Start

```bash
# Clone the repository
git clone https://github.com/Rich-T-kid/OptiSQL.git
cd OptiSQL

# Build and run Go backend
make go-run

# Build and run Rust backend
make rust-run

# Frontend setup and run
make frontend-setup    # Create venv and install dependencies
make frontend-run      # Run locally without Docker
# OR with Docker
make frontend-docker-build
make frontend-docker-run

# Run all tests
make test-all

# Verify everything (format, lint, test)
make pre-push
```

See `make help` for all available commands.

## Project Structure

```
OptiSQL/
├── src/
│   ├── Backend/
│   │   ├── opti-sql-go/          # Go implementation (primary)
│   │   │   ├── operators/        # Query operators (filter, join, aggr, etc.)
│   │   │   ├── phy-optimizer/    # Query optimization logic
│   │   │   └── substrait/        # Substrait integration
│   │   └── opti-sql-rs/          # Rust implementation (Go clone for learning)
│   │       ├── src/project/      # Core project logic
│   │       └── src/              # Query processing modules
│   └── FrontEnd/                 # Python/FastAPI HTTP server (C++ query processing in progress)
│       ├── app/                  # API endpoints and logic
│       ├── tests/                # Frontend tests
│       └── Dockerfile            # Docker configuration
├── .github/workflows/            # CI/CD pipelines
├── Makefile                      # Development commands
└── CONTRIBUTING.md               # Contribution guidelines
```

**Development Approach:**

Initial development is done in **Go** (`opti-sql-go`), which serves as the primary implementation. The **Rust** version (`opti-sql-rs`) is developed shortly after as a learning exercise and eventual performance-optimized alternative, closely mirroring the Go implementation.

**Key Directories:**
- `/operators` - SQL operator implementations (filter, join, aggregation, project)
- `/physical-optimizer` - Query plan parsing and optimization
- `/substrait` - Substrait plan integration
- `/operators/OPERATORS.md` - concise reference for operator constructors, behavior and examples

## Branching Model

We use a structured branching model to maintain stability and enable smooth collaboration:

- **`main`** - Production-ready code. Always stable and deployable. Represents released versions.
- **`pre-release`** - Accumulation branch for completed features awaiting release. Multiple features are merged here, tested together, and pushed to `main` as a single versioned release.
- **`feature/*`** - Individual feature development (e.g., `feature/hash-join`).
- **`fix/*`** - Bug fixes (e.g., `fix/null-handling`).

**Why This Model?**

This approach prevents unstable code from reaching `main`, simplifies rollbacks, and ensures all changes undergo proper testing and review before deployment. Feature branches isolate work, allowing focused reviews and parallel development without conflicts. The `pre-release` branch acts as a staging area where features are bundled together before being released as a new version.

**Workflow:**
1. Create a feature branch from `pre-release`
2. Implement your changes with tests
3. Open a PR to merge into `pre-release`
4. Once enough features accumulate (e.g., projectExec + filter), `pre-release` is merged into `main` as a new version

## Development

### Code Quality

All code quality checks are automated and enforced by CI:
- **Linting** - `golangci-lint` (Go), `clippy` (Rust)
- **Formatting** - `go fmt` (Go), `cargo fmt` (Rust)
- **Testing** - Unit tests required for all new code

**Style and linting rules are non-negotiable and handled by automated linters.** This ensures consistency across the codebase and reduces bikeshedding during code reviews.

### Pull Requests

- Keep PRs small and focused on a single logical change
- Include tests for new functionality
- Ensure all CI checks pass before requesting review
- Follow commit message conventions (see CONTRIBUTING.md)

### Running Checks Locally

Before pushing, verify your changes pass all checks:

```bash
make pre-push
```

This runs formatting, linting, and all tests.

## Contributing

Want to contribute? Check out [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines on:
- Writing and running tests
- PR format and commit message conventions
- Development workflow and tooling
- Build and run instructions

## License
This project is licensed under the terms specified in [LICENSE.txt](LICENSE.txt).