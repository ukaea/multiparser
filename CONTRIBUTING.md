# Contributing to Multiparser

Contributions to improve and enhance _Multiparser_ are very welcome, we ask that such modifications are made by firstly opening an issue outlining the fix or change allowing for discussion, then creating a feature branch on which to develop.

## :memo: Issue Creation

When opening an issue please ensure you outline in as much detail as possible the issues or proposed changes you wish to develop. Also ensure that this feature/fix has not already been raised by firstly searching through all current issues.

If reporting a bug, provide as much detail outlining a minimal example and describing any useful specifications and outlining the scenario which led to the problem.

## :ballot_box_with_check: Quality and Styling

### Linting and Formatting

This repository uses the `ruff` code formatter and linting to ensure consistency in styling. It is recommended that `ruff` be executed prior to committing changes either directly or by setting up git hooks via pre-commit.

### Security

The security check tool [_Bandit_](https://bandit.readthedocs.io/en/latest/) is executed within the continuous integration pipeline to check for the presence of any common security issues within the Python code.

### Testing and Coverage

_Multiparser_ contains a dedicated suite of tests created using [Pytest](https://docs.pytest.org/) for checking functionality and behaviour. The repository aims for a test coverage of > 90%.

### Documentation

To ensure functions, methods and classes are documented appropriately _Multiparser_ follows the Numpy docstring convention. The tool [_Interrogate_](https://pypi.org/project/interrogate/) is used to check docstring coverage.

## :arrow_right: Creating a Pull Request

Once you are satisfied with the changes ensure the test suite is run and all tests are passing before opening a merge request into `main`. Your branch should be named appropriately with a description of the issue/feature it addresses, e.g. `feature/add-new-feature` or `hotfix/fixed-bug-a`. If your code adds additional functionality be sure to add additional tests which test behaviour is as expected. In addition please update the `CHANGELOG.md` file describing the feature under the `Unreleased` heading, the feature will then be listed under the next release.
