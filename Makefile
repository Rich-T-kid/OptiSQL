.PHONY: help go-test rust-test go-run rust-run go-lint rust-lint go-fmt rust-fmt test-all lint-all fmt-all pre-push

# Default target
help:
	@echo "Available targets:"
	@echo "  make go-test       - Run Go tests"
	@echo "  make rust-test     - Run Rust tests"
	@echo "  make go-run        - Run Go application"
	@echo "  make rust-run      - Run Rust application"
	@echo "  make go-lint       - Run Go linter"
	@echo "  make rust-lint     - Run Rust linter and formatter check"
	@echo "  make go-fmt        - Format Go code"
	@echo "  make rust-fmt      - Format Rust code"
	@echo "  make test-all      - Run all tests (Go + Rust)"
	@echo "  make lint-all      - Run all linters (Go + Rust)"
	@echo "  make fmt-all       - Format all code (Go + Rust)"
	@echo "  make pre-push      - Run fmt, lint, and test (use before pushing)"

# Go targets
go-test:
	@echo "Running Go tests..."
	cd src/Backend/opti-sql-go && go test -v ./...

go-test-race:
	@echo "Running Go tests with race detector..."
	cd src/Backend/opti-sql-go && go test -race -v ./...

go-test-coverage:
	@echo "Running Go tests with coverage..."
	cd src/Backend/opti-sql-go && go test -v -coverprofile=coverage.out ./...
	cd src/Backend/opti-sql-go && go tool cover -func=coverage.out
go-run:
	@echo "Running Go application..."
	cd src/Backend/opti-sql-go && go run main.go

go-build:
	@echo "Building Go application..."
	cd src/Backend/opti-sql-go && go build -o opti-sql-go

go-lint:
	@echo "Running Go linter..."
	cd src/Backend/opti-sql-go && golangci-lint run ./...

go-fmt:
	@echo "Formatting Go code..."
	cd src/Backend/opti-sql-go && go fmt ./...

# Rust targets
rust-test:
	@echo "Running Rust tests..."
	cd src/Backend/opti-sql-rs && cargo test --verbose

rust-run:
	@echo "Running Rust application..."
	cd src/Backend/opti-sql-rs && cargo run

rust-build:
	@echo "Building Rust application..."
	cd src/Backend/opti-sql-rs && cargo build --release

rust-lint:
	@echo "Running Rust linter..."
	cd src/Backend/opti-sql-rs && cargo clippy --all-targets --all-features -- -D warnings

rust-fmt:
	@echo "Formatting Rust code..."
	cd src/Backend/opti-sql-rs && cargo fmt

rust-fmt-check:
	@echo "Checking Rust formatting..."
	cd src/Backend/opti-sql-rs && cargo fmt --check

# Combined targets
test-all: go-test rust-test
	@echo "All tests completed!"

lint-all: go-lint rust-lint
	@echo "All linting completed!"

fmt-all: go-fmt rust-fmt
	@echo "All formatting completed!"

# Pre-push verification
pre-push: fmt-all lint-all test-all
	@echo "All checks passed! Ready to push."

# Clean targets
clean-go:
	@echo "Cleaning Go build artifacts..."
	cd src/Backend/opti-sql-go && go clean
	rm -f src/Backend/opti-sql-go/opti-sql-go
	rm -f src/Backend/opti-sql-go/coverage.out

clean-rust:
	@echo "Cleaning Rust build artifacts..."
	cd src/Backend/opti-sql-rs && cargo clean

clean-all: clean-go clean-rust
	@echo "All build artifacts cleaned!"
