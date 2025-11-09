# Contributing to RoboSystems

Thank you for your interest in contributing to RoboSystems! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Process](#development-process)
- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Security](#security)

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/github-org/robosystems.git
   cd robosystems
   ```
3. **Add the upstream remote**:
   ```bash
   git remote add upstream https://github.com/RoboFinSystems/robosystems.git
   ```
4. **Set up your development environment** (see [Development Setup](#development-setup))

## Development Process

We use GitHub flow with automated tooling for our development process:

1. Create a feature branch using our tooling
2. Make your changes in small, atomic commits
3. Write or update tests for your changes
4. Update documentation as needed
5. Create a Claude-powered PR to the `main` branch

### Branch Creation and Naming

Use our automated branch creation tool with `just`:

```bash
# Create a new feature branch
just create-feature feature add-user-auth main

# Create a bugfix branch
just create-feature bugfix fix-connection-timeout main

# Create a hotfix branch
just create-feature hotfix critical-security-patch main

# Create a chore branch
just create-feature chore update-dependencies main

# Create a refactor branch
just create-feature refactor improve-error-handling main
```

**Branch Types:**

- `feature/` - New features or enhancements
- `bugfix/` - Bug fixes for existing functionality
- `hotfix/` - Critical fixes that need immediate attention
- `chore/` - Maintenance tasks (deps, configs, etc.)
- `refactor/` - Code refactoring without functional changes

**Note:** All PRs must target the `main` branch only.

## How to Contribute

### Reporting Bugs

Before creating a bug report, please check existing issues to avoid duplicates. When creating a bug report, include:

- Clear, descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- System information (OS, Python version, etc.)
- Relevant logs or error messages

### Suggesting Enhancements

Enhancement suggestions are welcome! Please provide:

- Clear, descriptive title
- Detailed description of the proposed feature
- Use cases and benefits
- Possible implementation approach (if applicable)

### First-Time Contributors

Look for issues labeled `good first issue` or `help wanted`. These are great starting points for new contributors.

## Development Setup

### Prerequisites

- Docker and Docker Compose
- `uv` for Python package and version management
- `rust-just` command runner (installed via uv)

### Local Development Environment

1. **Install development tools**:

   ```bash
   # Install uv (Python package and version manager)
   curl -LsSf https://astral.sh/uv/install.sh | sh
   # Or on macOS with Homebrew: brew install uv

   # Install just (command runner)
   uv tool install rust-just
   ```

2. **Set up Python environment**:

   ```bash
   # uv automatically handles Python version from .python-version
   just init
   ```

3. **Start the development stack**:

   ```bash
   just start
   ```

4. **Run tests**:
   ```bash
   just test
   ```

### Environment Configuration

Copy the example environment file and configure:

```bash
cp .env.example .env
# Edit .env with your configuration
```

## Coding Standards

### Python Code Style

- **Formatter**: Black with 88 character line length
- **Linter**: Ruff
- **Type checking**: Pyright/Basedpyright
- **Docstrings**: Google style
- **Import sorting**: Ruff's isort rules

Run code quality checks:

```bash
just lint      # Run linting and formatting
just format    # Auto-format code
just typecheck # Run type checking
```

### Commit Messages

Follow the Conventional Commits specification:

```
type(scope): subject

body (optional)

footer (optional)
```

Types:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Maintenance tasks
- `perf`: Performance improvements

Examples:

```
feat(api): add portfolio analysis endpoint
fix(kuzu): resolve connection pooling issue
docs(readme): update deployment instructions
```

### Code Organization

- Follow existing project structure
- Keep files focused and single-purpose
- Use descriptive names for functions, variables, and classes
- Add type hints to all function signatures
- Write docstrings for all public functions and classes

## Testing

### Test Requirements

- All new features must include tests
- Bug fixes should include regression tests
- Maintain or improve code coverage
- Tests must pass locally before submitting PR

### Running Tests

```bash
# Run all tests
just test-all

# Run specific test file
just test tests/test_specific.py

# Run with coverage
just test-cov

# Run only unit tests
pytest -m unit

# Run integration tests
pytest -m integration
```

### Writing Tests

- Use pytest fixtures for reusable test data
- Mock external dependencies
- Use descriptive test names that explain what is being tested
- Group related tests in classes
- Add appropriate markers (`@pytest.mark.unit`, `@pytest.mark.integration`)

Example test structure:

```python
import pytest
from unittest.mock import Mock, patch

class TestFeatureName:
    """Tests for FeatureName functionality."""

    @pytest.fixture
    def setup_data(self):
        """Fixture for test data."""
        return {"key": "value"}

    def test_feature_success_case(self, setup_data):
        """Test feature works correctly with valid input."""
        # Arrange
        expected = "expected_result"

        # Act
        result = feature_function(setup_data)

        # Assert
        assert result == expected

    def test_feature_error_case(self):
        """Test feature handles errors appropriately."""
        with pytest.raises(ValueError):
            feature_function(invalid_data)
```

## Documentation

### Documentation Requirements

- Update README.md for significant changes
- Add docstrings to new functions and classes
- Update API documentation for endpoint changes
- Include inline comments for complex logic
- Update configuration examples if needed

### API Documentation

API documentation is auto-generated from FastAPI routes. Ensure:

- Proper type hints on all parameters
- Descriptive docstrings on route functions
- Response models are well-defined
- Example requests/responses where helpful

## Pull Request Process

### Creating a Pull Request

We use an automated Claude-powered PR creation process:

```bash
# Create a PR with Claude analysis and review (default)
just create-pr

# Create a PR targeting main with Claude review
just create-pr main true

# Create a PR without Claude review (faster)
just create-pr main false
```

This will:

1. Analyze your changes using Claude AI
2. Generate a comprehensive PR description
3. Include test results and impact analysis
4. Create the PR on GitHub automatically

### Before Creating a PR

1. **Commit all changes**:

   ```bash
   git add .
   git commit -m "feat: your descriptive commit message"
   ```

2. **Update from upstream**:

   ```bash
   git fetch origin
   git rebase origin/main
   ```

3. **Run all checks locally**:

   ```bash
   just test-all
   just lint
   just format
   just typecheck
   ```

4. **Push your branch**:
   ```bash
   git push origin your-branch-name
   ```

### PR Requirements

- All tests must pass
- Code must pass linting and formatting checks
- Must not decrease test coverage significantly
- Must include appropriate documentation updates
- Claude review is recommended for complex changes
- Must be reviewed by at least one maintainer

### Manual PR Creation

If needed, you can create a PR manually:

```bash
gh pr create --base main --title "Your PR title" --body "Your PR description"
```

### Review Process

1. Claude will analyze and create your PR automatically
2. Review the generated PR description and make adjustments
3. Address reviewer feedback promptly
4. Keep PR focused - one feature/fix per PR
5. Update PR based on feedback
6. Maintainer will merge once approved

## Release Process

### Creating a Release

Releases are created using our automated tooling:

```bash
# Create a patch release and deploy to staging
just create-release patch staging

# Create a minor release and deploy to staging
just create-release minor staging

# Create a major release (no auto-deploy)
just create-release major none
```

This will:

1. Create a release branch
2. Update version numbers
3. Generate changelog
4. Create a release PR
5. Optionally deploy to staging for testing

## Security

### Security Vulnerabilities

**DO NOT** create public issues for security vulnerabilities. Instead:

1. Email security@robosystems.ai with details
2. Include steps to reproduce if possible
3. Allow time for the issue to be addressed before public disclosure

### Security Best Practices

- Never commit secrets or credentials
- Use environment variables for sensitive configuration
- Validate and sanitize all user inputs
- Keep dependencies up to date
- Follow OWASP guidelines for web security

## CloudFormation Contributions

### Template Guidelines

- Use YAML format for CloudFormation templates
- Include comprehensive parameter descriptions
- Add helpful default values where appropriate
- Use conditions for environment-specific resources
- Include proper tags on all resources

### Testing CloudFormation Changes

```bash
# Lint and validate CloudFormation templates
just cf-lint api
just cf-lint worker
```

## Questions and Support

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and discussions
- **Email**: hello@robosystems.ai for other inquiries

## Recognition

Contributors will be recognized in our [Contributors](https://github.com/RoboFinSystems/robosystems/graphs/contributors) page.

Thank you for contributing to RoboSystems! 🚀
