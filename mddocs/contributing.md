# Contributing Guide

Welcome! There are many ways to contribute, including submitting bug reports, improving documentation, submitting feature requests, reviewing new submissions, or contributing code that can be incorporated into the project.

## Review process

For any **significant** changes please create a new GitHub issue and
enhancements that you wish to make. Describe the feature you would like
to see, why you need it, and how it will work. Discuss your ideas
transparently and get community feedback before proceeding.

Small changes can directly be crafted and submitted to the GitHub
Repository as a Pull Request. This requires creating a **repo fork** using
[instruction](https://docs.github.com/en/get-started/quickstart/fork-a-repo)

## Initial setup for local development

### Install Git

Please follow [instruction](https://docs.github.com/en/get-started/quickstart/set-up-git).

### Clone the repo

Open terminal and run these commands to clone a **forked** repo:

```bash
git clone https://github.com/MobileTeleSystems/data-rentgen -b develop

cd data_rentgen
```

### Setup environment

Firstly, install [make](https://www.gnu.org/software/make/manual/make.html). It is used for running complex commands in local environment.

Secondly, create virtualenv and install dependencies:

```bash
make venv
```

If you already have venv, but need to install dependencies required for development:

```bash
make venv-install
```

We are using [uv](https://docs.astral.sh/uv/) for managing dependencies and building the package.
It allows to keep development environment the same for all developers due to using lock file with fixed dependency versions.

There are *extra* dependencies (included into package as optional):

* `server`
* `consumer`
* `http2kafka`
* `postgres`
* `gssapi`
* `seed`

And *groups* (not included into package, used locally and in CI):

* `test` - for running tests
* `dev` - for development, like linters, formatters, mypy, pre-commit and so on
* `docs` - for building documentation

### Enable pre-commit hooks

[pre-commit](https://pre-commit.com/) hooks allows to validate & fix repository content before making new commit.
It allows to run linters, formatters, fix file permissions and so on. If something is wrong, changes cannot be committed.

Firstly, install [prek](https://prek.j178.dev/):

```bash
prek install --install-hooks
```

Ant then test hooks run:

```bash
prek run
```

## How to

### Run development instance locally

Start DB container & seed database with some examples:

```bash
make db db-seed
```

Then start development server:

```bash
make dev-server
```

And open [http://localhost:8000/docs](http://localhost:8000/docs)

Settings are stored in `.env.local` file.

To start developlment consumer, open a new terminal window/tab, and run:

```bash
make broker dev-consumer
```

### Working with migrations

Start database:

```bash
make db-start
```

Generate revision:

```bash
make db-revision ARGS="-m 'Message'"
```

Upgrade db to `head` migration:

```bash
make db-upgrade
```

Downgrade db to `head-1` migration:

```bash
make db-downgrade
```

### Run tests locally

This is as simple as:

```bash
make test
```

This command starts all necessary containers (Postgres, Kafka), runs all necessary migrations, and then runs Pytest.

You can pass additional arguments to pytest like this:

```bash
make test PYTEST_ARGS="-m client-sync -lsx -vvvv --log-cli-level=INFO"
```

Stop all containers and remove created volumes:

```bash
make test-cleanup ARGS="-v"
```

Get fixtures not used by any test:

```bash
make test-check-fixtures
```

### Run production instance locally

Firstly, build production image:

```bash
make prod-build
```

And then start it:

```bash
make prod
```

Then open [http://localhost:8000/docs](http://localhost:8000/docs)

Settings are stored in `.env.docker` file.

### Build documentation

Build documentation using Sphinx & open it:

```bash
make docs
```

If documentation should be build cleanly instead of reusing existing build result:

```bash
make docs-fresh
```

### Create pull request

Commit your changes:

```bash
git commit -m "Commit message"
git push
```

Then open Github interface and [create pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-a-pull-request).
Please follow guide from PR body template.

After pull request is created, it get a corresponding number, e.g. 123 (`pr_number`).

### Write release notes

Data.Rentgen uses [towncrier](https://pypi.org/project/towncrier/) for changelog management.

To submit a change note about your PR, add a text file into the [docs/changelog/next_release](./next_release) folder. It should contain an explanation of what applying this PR will change in the way end-users interact with the project. One sentence is usually enough but feel free to add as many details as you feel necessary for the users to understand what it means.

**Use the past tense** for the text in your fragment because, combined with others, it will be a part of the “news digest” telling the readers **what changed** in a specific version of the library *since the previous version*.

reStructuredText syntax for highlighting code (inline or block), linking parts of the docs or external sites. If you wish to sign your change, feel free to add `-- by :user:`github-username` at the end (replace `github-username` with your own!).

Finally, name your file following the convention that Towncrier understands: it should start with the number of an issue or a PR followed by a dot, then add a patch type, like `feature`, `doc`, `misc` etc., and add `.rst` as a suffix. If you need to add more than one fragment, you may add an optional sequence number (delimited with another period) between the type and the suffix.

In general the name will follow `<pr_number>.<category>.rst` pattern, where the categories are:

* `feature`: Any new feature. Adding new functionality that has not yet existed.
* `removal`: Signifying a deprecation or removal of public API.
* `bugfix`: A bug fix.
* `improvement`: An improvement. Improving functionality that already existed.
* `doc`: A change to the documentation.
* `dependency`: Indicates that there have been changes in dependencies.
* `misc`: Changes internal to the repo like CI, test and build changes.
* `breaking`: introduces a breaking API change.
* `significant`: Indicates that significant changes have been made to the code.

A pull request may have more than one of these components, for example
a code change may introduce a new feature that deprecates an old
feature, in which case two fragments should be added. It is not
necessary to make a separate documentation fragment for documentation
changes accompanying the relevant code changes.

#### Examples for adding changelog entries to your Pull Requests

```rst title="docs/changelog/next_release/1234.doc.1.rst"
Added a ``:github:user:`` role to Sphinx config -- by :github:user:`someuser`
```

```rst title="docs/changelog/next_release/2345.bugfix.rst"
Fixed behavior of ``server`` -- by :github:user:`someuser`
```

```rst title="docs/changelog/next_release/3456.feature.rst"
Added support of ``timeout`` in ``LDAP``
-- by :github:user:`someuser`, :github:user:`anotheruser` and :github:user:`otheruser`
```

!!! tip
    See `pyproject.toml <pyproject.toml>`_ for all available categories (`tool.towncrier.type`).
    Towncrier philosophy:
    https://towncrier.readthedocs.io/en/stable/#philosophy

#### How to skip change notes check?

Just add `ci:skip-changelog` label to pull request.

#### Release Process

!!! note
    This is for repo maintainers only

Before making a release from the `develop` branch, follow these steps:

0. Checkout to `develop` branch and update it to the actual state

 ```bash
 git checkout develop
 git pull -p
 ```

1. Backup `NEXT_RELEASE.rst`

 ```bash
 cp "docs/changelog/NEXT_RELEASE.rst" "docs/changelog/temp_NEXT_RELEASE.rst"
 ```

2. Build the Release notes with Towncrier

 ```bash
 VERSION=$(poetry version -s)
 towncrier build "--version=${VERSION}" --yes
 ```

3. Change file with changelog to release version number

 ```bash
 mv docs/changelog/NEXT_RELEASE.rst "docs/changelog/${VERSION}.rst"
 ```

4. Remove content above the version number heading in the `${VERSION}.rst` file

 ```bash
awk '!/^.*towncrier release notes start/' "docs/changelog/${VERSION}.rst" > temp && mv temp "docs/changelog/${VERSION}.rst"
 ```

5. Update Changelog Index

 ```bash
 awk -v version=${VERSION} '/DRAFT/{print;print "    " version;next}1' docs/changelog/index.rst > temp && mv temp docs/changelog/index.rst
 ```

6. Restore `NEXT_RELEASE.rst` file from backup

 ```bash
 mv "docs/changelog/temp_NEXT_RELEASE.rst" "docs/changelog/NEXT_RELEASE.rst"
 ```

7. Commit and push changes to `develop` branch

 ```bash
 git add .
 git commit -m "Prepare for release ${VERSION}"
 git push
 ```

8. Merge `develop` branch to `master`, **WITHOUT** squashing

 ```bash
 git checkout master
 git pull
 git merge develop
 git push
 ```

9. Add git tag to the latest commit in `master` branch

 ```bash
 git tag "$VERSION"
 git push origin "$VERSION"
 ```

10. Update version in `develop` branch **after release**:

 ```bash
 git checkout develop

 NEXT_VERSION=$(echo "$VERSION" | awk -F. '/[0-9]+\./{$NF++;print}' OFS=.)
 poetry version "$NEXT_VERSION"

 git add .
 git commit -m "Bump version"
 git push
 ```
