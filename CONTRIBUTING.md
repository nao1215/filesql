# Contributing Guide

## Introduction

Thank you for considering contributing to the filesql project! This document explains how to contribute to the project. We welcome all forms of contributions, including code contributions, documentation improvements, bug reports, and feature suggestions.

## Setting Up Development Environment

### Prerequisites

#### Installing Go

filesql development requires Go 1.24 or later.

**macOS (using Homebrew)**
```bash
brew install go
```

**Linux (for Ubuntu)**
```bash
# Using snap
sudo snap install go --classic

# Or download from official site
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
source ~/.profile
```

**Windows**
Download and run the installer from the [official Go website](https://go.dev/dl/).

Verify installation:
```bash
go version
```

### Cloning the Project

```bash
git clone https://github.com/nao1215/filesql.git
cd filesql
```

### Installing Development Tools

```bash
# Install necessary development tools
make tools
```

### Verification

To verify that your development environment is set up correctly, run the following commands:

```bash
# Run tests
make test

# Run linter
make lint
```

## Development Workflow

### Branch Strategy

- `main` branch is the latest stable version
- Create new branches from `main` for new features or bug fixes
- Branch naming examples:
  - `feature/add-json-support` - New feature
  - `fix/issue-123` - Bug fix
  - `docs/update-readme` - Documentation update

### Coding Standards

This project follows these standards:

1. **Conform to [Effective Go](https://go.dev/doc/effective_go)**
2. **Avoid using global variables** (except for config package)
3. **Always add comments to public functions, variables, and structs**
4. **Keep functions as small as possible**
5. **Writing tests is encouraged**

### Writing Tests

Tests are important. Please follow these guidelines:

1. **Unit tests**: Aim for 80% or higher coverage
2. **Test readability**: Write clear test cases
3. **Parallel execution**: Use `t.Parallel()` whenever possible

Test example:
```go
func TestFile_Parse(t *testing.T) {
    t.Parallel()
    
    t.Run("should parse CSV file correctly", func(t *testing.T) {
        // Clear input and expected values for test case
        input := "name,age\nAlice,30"
        expected := &Table{...}
        
        result, err := ParseCSV(input)
        assert.NoError(t, err)
        assert.Equal(t, expected, result)
    })
}
```

## Creating Pull Requests

### Preparation

1. **Check or Create Issues**
   - Check if there are existing issues
   - For major changes, we recommend discussing the approach in an issue first

2. **Write Tests**
   - Always add tests for new features
   - For bug fixes, create tests that reproduce the bug

3. **Quality Check**
   ```bash
   # Ensure all tests pass
   make test
   
   # Linter check
   make lint
   
   # Check coverage (80% or higher)
   go test -cover ./...
   ```

### Submitting Pull Request

1. Create a Pull Request from your forked repository to the main repository
2. PR title should briefly describe the changes
3. Include the following in PR description:
   - Purpose and content of changes
   - Related issue number (if any)
   - Test method
   - Reproduction steps for bug fixes

### About CI/CD

GitHub Actions automatically checks the following items:

- **Cross-platform testing**: Test execution on Linux, macOS, and Windows
- **Linter check**: Static analysis with golangci-lint
- **Test coverage**: Maintain 80% or higher coverage
- **Build verification**: Successful builds on each platform

Merging is not possible unless all checks pass.

## Bug Reports

When you find a bug, please create an issue with the following information:

1. **Environment Information**
   - OS (Linux/macOS/Windows) and version
   - Go version
   - filesql version

2. **Reproduction Steps**
   - Minimal code example to reproduce the bug
   - Data files used (if possible)

3. **Expected and Actual Behavior**

4. **Error Messages or Stack Traces** (if any)

## Contributing Outside of Coding

The following activities are also greatly welcomed:

### Activities that Boost Motivation

- **Give a GitHub Star**: Show your interest in the project
- **Promote the Project**: Introduce it in blogs, social media, study groups, etc.
- **Become a GitHub Sponsor**: Support available at [https://github.com/sponsors/nao1215](https://github.com/sponsors/nao1215)

### Other Ways to Contribute

- **Documentation Improvements**: Fix typos, improve clarity of explanations
- **Translations**: Translate documentation to new languages
- **Add Examples**: Provide practical sample code
- **Feature Suggestions**: Share new feature ideas in issues

## Community

### Code of Conduct

Please refer to [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). We expect all contributors to treat each other with respect.

### Questions and Reports

- **GitHub Issues**: Bug reports and feature suggestions

## License

Contributions to this project are considered to be released under the project's license (MIT License).

---

Thank you again for considering contributing! We sincerely look forward to your participation.