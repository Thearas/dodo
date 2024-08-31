# AGENTS.md - AI Development Guidelines for Dodo

> This document provides guidelines for AI/LLM agents working on the Dodo codebase.

## Project Overview

**Dodo** is a CLI tool for Doris database operations, written in Go 1.25+. Main features:
- Deploy Doris clusters
- Dump schema and audit logs
- Generate fake data for tables (AI-powered)
- Replay audit logs or SQL files
- Anonymize SQL queries

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Go 1.25+ |
| CLI Framework | [Cobra](https://github.com/spf13/cobra) |
| Configuration | [Viper](https://github.com/spf13/viper) |
| SQL Parsing | [ANTLR4](https://github.com/antlr4-go/antlr/v4) (custom Doris parser) |
| Database Driver | [sqlx](https://github.com/jmoiron/sqlx) with MySQL driver |
| LLM Integration | [openai-go](https://github.com/openai/openai-go) |
| Logging | [logrus](https://github.com/sirupsen/logrus) |
| Testing | Standard `testing` package with [testify](https://github.com/stretchr/testify) |

## Project Structure

```
dodo/
├── main.go              # Entry point
├── cmd/                 # CLI commands (Cobra)
│   ├── root.go          # Root command and global flags
│   ├── dump.go          # Dump command
│   ├── replay.go        # Replay command
│   ├── gendata.go       # Data generation command
│   └── ...
├── src/                 # Core business logic
│   ├── db.go            # Database operations
│   ├── llm.go           # LLM integration (OpenAI/DeepSeek)
│   ├── gendata.go       # Data generation logic
│   ├── replay.go        # SQL replay logic
│   ├── anonymizer.go    # SQL anonymization
│   ├── generator/       # Data generators for different types
│   ├── parser/          # ANTLR-based SQL parser (auto-generated)
│   ├── prompt/          # LLM prompt templates
│   └── pyspark/         # Spark SQL support
├── example/             # Example configurations
├── fixture/             # Test fixtures
└── output/              # Default output directory (generated)
```

## Coding Conventions

### Go Code Style

1. **Follow standard Go conventions**
   - Use `gofmt` and `goimports` for formatting
   - Use `golangci-lint` for linting
   - Run `make fmt` and `make lint` before committing

2. **Error handling**
   ```go
   // Good: wrap errors with context
   if err != nil {
       return fmt.Errorf("failed to parse table %s: %w", tableName, err)
   }
   
   // Bad: bare error return
   if err != nil {
       return err
   }
   ```

3. **Logging**
   ```go
   // Use logrus with appropriate levels
   logrus.Debugf("Processing table: %s", tableName)
   logrus.Infof("Completed %d rows", count)
   logrus.Warnf("Skipping invalid column: %s", colName)
   logrus.Errorf("Failed to connect: %v", err)
   ```

4. **Imports organization**
   ```go
   import (
       // Standard library
       "context"
       "fmt"
       
       // Third-party
       "github.com/samber/lo"
       "github.com/sirupsen/logrus"
       
       // Internal (local module)
       "github.com/Thearas/dodo/src"
   )
   ```

5. **Use `lo` (samber/lo) for collection operations**
   ```go
   // Prefer lo functions over manual loops
   filtered := lo.Filter(items, func(item Item, _ int) bool {
       return item.Active
   })
   ```

### CLI Commands (Cobra)

1. **Place commands in `cmd/` directory**
2. **Use persistent flags for shared options in `root.go`**
3. **Register flag completion functions for better UX**
4. **Support config file (`$HOME/.dodo.yaml`) and environment variables (`DODO_*`)**

### Testing

1. **Test files**: `*_test.go` alongside source files
2. **Use `testify/assert`** for assertions
3. **Run tests**: `make test` or `go test -v ./...`
4. **Test fixtures in `fixture/` directory**

### Format & Lint

- Run `make fmt` to format code
- Run `make lint` to check for linting issues

## Build & Development

```bash
# Build
make build              # Build binary
make install            # Build and install to /usr/local/bin

# Development
make run <args>         # Run with arguments
make test               # Run all tests
make fmt                # Format code
make lint               # Run linter

# Code generation
make gen                # Regenerate parser and prompts
make gen-parser         # Regenerate ANTLR parser only
make gen-prompt         # Regenerate prompt templates only
```

## Key Patterns

### 1. Configuration via Viper

```go
// Flags are bound to viper for config file/env support
viper.SetEnvPrefix("DODO")
viper.AutomaticEnv()
```

### 2. Context Propagation

```go
// Always pass context for cancellation support
func DoWork(ctx context.Context, ...) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        // do work
    }
}
```

## Important Files

| File | Purpose |
|------|---------|
| [cmd/root.go](cmd/root.go) | Global configuration and flags |
| [src/db.go](src/db.go) | Database connection and queries |
| [src/llm.go](src/llm.go) | LLM function calling for AI features |
| [src/gendata.go](src/gendata.go) | Data generation orchestration |
| [src/generator/generator.go](src/generator/generator.go) | Generator interface and registry |
| [src/parser/doris_parser.go](src/parser/doris_parser.go) | Auto-generated Doris SQL parser |
| [src/prompt/](src/prompt/) | LLM prompt templates |

## Do's and Don'ts

### Do's ✅

- Run `make fmt` and `make lint` before committing
- Always add tests for new features
- Use context for cancellation
- Wrap errors with context using `fmt.Errorf("...: %w", err)` or `errors.New()` if no formatting args
- Use `logrus` for logging (not `fmt.Print`)
- Follow existing patterns in the codebase
- Update README.md for user-facing changes
- **Reuse existing common logic** - always search for existing utilities before implementing new ones, common logic usually in `src/` and `src/misc.go`

### Don'ts ❌

- Don't modify auto-generated files in `src/parser/` directly
- Don't use `panic` for error handling (except truly unrecoverable)
- Don't hardcode paths; use flags or config
- Don't ignore errors silently
- Don't add direct dependencies without checking existing ones
- **Don't reinvent the wheel** - check `src/misc.go`, `src/db.go`, and other files for reusable functions before writing new implementations

## Adding New Features

### New CLI Command

1. Run `make addcmd -- <command>` to create `cmd/<command>.go`
2. Add command to parent (usually `rootCmd`)
3. Implement `RunE` function
4. Add flags and completion functions

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DODO_HOST` | Database host | `127.0.0.1` |
| `DODO_PORT` | Database port | `9030` |
| `DODO_USER` | Database user | `root` |
| `DODO_PASSWORD` | Database password | (empty) |
| `DODO_LOG_LEVEL` | Log level (trace/debug/info/warn) | `info` |
| `DORIS_YES` | Skip confirmation prompts (set to 1) | (unset) |

> **Tip for AI agents**: Set `DORIS_YES=1` before running dodo commands to skip interactive confirmation prompts.

## References

- [README.md](README.md) - User documentation
- [introduction.md](introduction.md) - Detailed usage guide
- [introduction-zh.md](introduction-zh.md) - Chinese documentation
- [example/](example/) - Example configurations
