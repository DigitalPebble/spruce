source .venv/bin/activate
zensical serve

The site is built from a release tag by `.github/workflows/documentation.yml`, either when a GitHub release is published or by hand for a given tag, so it describes a release rather than `main`.
