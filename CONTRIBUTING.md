# 🛠️ Contribution Guide

Thank you for considering contributing to this project!

## Getting Started

- Create a new branch from `main`
- Use a meaningful name, e.g. `feature/new-functionality`

## Development Environment

If you're using the Devcontainer setup:

1. Open the terminal inside the Devcontainer
2. Configure your Git identity:

```bash
git config --global user.name "Your Name"
git config --global user.email "your@email.com"
```

3. Authenticate with GitHub CLI:

```bash
gh auth login
```

Choose "GitHub.com" → "HTTPS" → "Login with browser" and follow the instructions.

## Code Style

- Follow the project's coding conventions
- Use linting tools if available (e.g. Spotless for Groove)

### Testing

- Make sure all tests pass using:

```bash
./gradlew test
```

### To build
```bash
./gradlew --stop
./gradlew clean build
```

## Pull Requests

- Use clear titles and descriptions
- Reference related issues if applicable
- Keep commits clean and descriptive (semantic commits preferred)

## Additional Notes

- This project uses Java 17 and Gradle
- Contributions to documentation, CI, and tooling are welcome
- Please avoid committing personal credentials or configuration data
