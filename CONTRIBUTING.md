# Contributing to MLOps Mid-Project: Customer Churn Prediction

First off, thank you for considering contributing to our MLOps project! It's people like you that make this project such a great tool for the community. We welcome contributions from everyone, whether it's in the form of bug reports, feature requests, documentation improvements, or code contributions.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
   - [Issues](#issues)
   - [Pull Requests](#pull-requests)
3. [Development Setup](#development-setup)
4. [Coding Standards](#coding-standards)
5. [Testing](#testing)
6. [Documentation](#documentation)
7. [Commit Messages](#commit-messages)
8. [Versioning](#versioning)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to yotam.aflalo433@gmail.com .

## Getting Started

### Issues

- Feel free to submit issues and enhancement requests.
- Before creating an issue, please check that a similar issue doesn't already exist.
- When you create a new issue, please provide as much context as possible, including your environment, steps to reproduce, and any relevant logs or screenshots.

### Pull Requests

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

## Development Setup

To set up the development environment:

1. Clone the repository:
   ```
   git clone https://github.com/your-username/mlops-mid-project.git
   ```
2. Navigate to the project directory:
   ```
   cd mlops-mid-project
   ```
3. Create a virtual environment:
   ```
   python -m venv venv
   ```
4. Activate the virtual environment:
   - On Windows: `venv\Scripts\activate`
   - On Unix or MacOS: `source venv/bin/activate`
5. Install the dependencies:
   ```
   pip install -r requirements.txt
   ```

## Coding Standards

We follow PEP 8 style guide for Python code. Please ensure your code adheres to these standards.

- Use 4 spaces for indentation.
- Use meaningful variable and function names.
- Keep lines under 79 characters long.
- Write docstrings for all functions, classes, and modules.

We use `flake8` for linting. You can run it with:

```
flake8 .
```

## Testing

We use `pytest` for our test suite. Please write tests for new code you create. You can run the tests with:

```
pytest
```

Ensure all tests pass before submitting a pull request.

## Documentation

- Keep the README.md and other documentation up to date with changes.
- Use clear and concise language.
- Provide examples where appropriate.

## Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your-username/mlops-mid-project/tags).

Thank you for your contributions!
