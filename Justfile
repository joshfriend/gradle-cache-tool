set positional-arguments := true
set shell := ["bash", "-c"]

VERSION := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
RELEASE := "dist"
GOARCH := env("GOARCH", `go env GOARCH`)
GOOS := env("GOOS", `go env GOOS`)

_help:
    @just -l

# Run tests
test:
    go test -run ^Test ./...

# Lint code
lint:
    golangci-lint run

# Format code
fmt:
    git ls-files | grep '\.go$' | xargs gofmt -w
    go mod tidy

# Build for current platform
build GOOS=(GOOS) GOARCH=(GOARCH):
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{ RELEASE }}
    echo "Building dist/gradle-cache-{{ GOOS }}-{{ GOARCH }}"
    CGO_ENABLED=0 GOOS={{ GOOS }} GOARCH={{ GOARCH }} \
        go build -trimpath -o {{ RELEASE }}/gradle-cache-{{ GOOS }}-{{ GOARCH }} \
        -ldflags "-s -w -X main.version={{ VERSION }}" \
        ./cmd/gradle-cache
    test "{{ GOOS }}-{{ GOARCH }}" = "$(go env GOOS)-$(go env GOARCH)" && \
        (cd {{ RELEASE }} && ln -sf gradle-cache-{{ GOOS }}-{{ GOARCH }} gradle-cache)
    echo "Done"

# Release: build all platforms and publish to GitHub via goreleaser
release:
    goreleaser release --clean

# Preview what a release would produce without publishing
release-dry:
    goreleaser release --snapshot --clean

# Clean build artifacts
clean:
    rm -rf {{ RELEASE }}
