# Release checklist

Bulklink releases are built from immutable Git tags and published through PyPI Trusted
Publishing. The repository does not store PyPI passwords or long-lived API tokens.

## Prepare a release candidate

1. Confirm the working tree contains only intended release changes.
2. Choose the next PEP 440 version. The current candidate is `0.2.0rc1`.
3. Update the version in both `pyproject.toml` and `bulklink.__version__`.
4. Move completed notes from `Unreleased` into a dated changelog section matching the
   exact version.
5. Run `./scripts/ci.sh` from a clean development environment.
6. Commit the candidate changes and wait for every job in the main CI workflow to pass.
7. Confirm the public exports in `bulklink.__all__` are intentional.
8. Create an annotated tag whose name is exactly `v` followed by the package version.
9. Push only that tag after the candidate commit is already present on `main`.
10. Review and approve the protected `pypi` environment when the release workflow asks.
11. Confirm that PyPI contains the same wheel and source archive produced by the workflow.

For this candidate:

```bash
git tag -a v0.2.0rc1 -m "Bulklink 0.2.0rc1"
git push origin v0.2.0rc1
```

## Required repository configuration

Before the first publication:

- configure a PyPI Trusted Publisher for repository `igors93/bulklink`;
- set the workflow name to `release.yml`;
- set the GitHub environment to `pypi`;
- protect the `pypi` environment with a required reviewer;
- do not add a `PYPI_TOKEN`, password, or username secret.

The release workflow grants `id-token: write` only to the publishing job. Build and test
jobs retain read-only repository permissions.

## Version consistency

The project version is intentionally present in both `pyproject.toml` and
`bulklink.__version__`. Contract tests and the release verifier fail when the values do
not match.

The release verifier also checks that an optional `BULKLINK_RELEASE_TAG` value exactly
matches `v{version}` and that the changelog contains a dated section for the version.

## Distribution verification

The release verifier checks:

- safe archive paths and absence of cache files;
- SPDX license metadata and the packaged `LICENSE` file;
- inclusion of the PEP 561 `py.typed` marker;
- installation of the wheel into a temporary virtual environment;
- runtime use of execution, resizing, diagnostics, events, shutdown, and the registry;
- strict type checking of a consumer against the installed wheel rather than `src/`.

The release workflow publishes the already verified files from `dist/`. It must never
rebuild artifacts in the publishing job.

## Promote a candidate to a final release

A final release is a new version, not a renamed candidate artifact. Update the project
to `0.2.0`, create a matching changelog section, rerun all checks, and publish a new
`v0.2.0` tag. Do not reuse or replace `v0.2.0rc1`.
