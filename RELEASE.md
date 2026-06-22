## Release process

`dbt-singlestore` releases are automated through GitHub Actions. A new package release is published to PyPI when a new version tag is pushed to the GitHub repository.

### Prerequisites

Before creating a release tag:

* Make sure the release changes are merged into the default branch.
* Make sure CI is passing.
* Update the package version in `setup.py` and `__version__.py` files.
* Update `README` changelog and `Testing and supported versions` block.
* The version tag must match the package version, using the `vX.Y.Z` format.

For example, package version `1.10.0` should be released with tag `v1.10.0`.

### Creating a release

From the default branch, run:

```bash
git checkout master
git pull origin master
git tag vX.Y.Z
git push origin vX.Y.Z
```

Replace `X.Y.Z` with the version being released.

After the tag is pushed, GitHub Actions will automatically build the package and publish it to PyPI.

### Verifying the release

After the release workflow finishes successfully:

1. Check that the GitHub Actions release workflow completed without errors.
2. Verify that the new version is available on PyPI.
3. Optionally install the released package locally:

```bash
pip install dbt-singlestore==X.Y.Z
```

### Failed releases

If the release workflow fails before publishing to PyPI, fix the issue and rerun the workflow or recreate the tag as needed.

If the package was already published to PyPI, do not reuse the same version number. PyPI package versions are immutable, so a fix must be released with a new version.
