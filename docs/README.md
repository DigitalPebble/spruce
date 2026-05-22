source .venv/bin/activate
zensical serve

## Versioned docs

Versioning uses [mike](https://github.com/squidfunk/mike) (Zensical fork). Install it with:

    pip install "mike @ git+https://github.com/squidfunk/mike.git"

Preview the versioned site (with the version selector) locally:

    mike serve

Releases are published automatically by `.github/workflows/documentation.yml`: pushes to `main` update the `dev` alias, and publishing a GitHub release publishes a numbered version (the release tag) aliased to `latest`.

https://zensical.org/docs/setup/versioning/


