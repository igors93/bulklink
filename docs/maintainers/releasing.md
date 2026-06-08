# Release checklist

Bulklink releases are built from immutable Git tags and published through PyPI Trusted
Publishing. The repository does not store PyPI passwords or long-lived API tokens.

## Release `0.3.0`

1. Confirm the feature commit passed Linux, Windows, and macOS CI.
2. Install the current stable release from PyPI in a clean environment.
3. Confirm absolute-deadline admission preserves FIFO, cancellation, and shutdown behavior.
4. Update the project and runtime versions to `0.3.0`.
5. Add a dated `0.3.0` changelog section while retaining all earlier release history.
6. Review `bulklink.__all__`, public enum values, dataclass fields, exception inheritance,
   and primary method calling conventions.
7. Run `./scripts/ci.sh` from a clean development environment.
8. Commit the release and wait for every main CI job to pass.
9. Create and push the annotated `v0.3.0` tag.
10. Review and approve the protected `pypi` environment.
11. Confirm PyPI contains the verified wheel and source archive from the workflow.

```bash
git tag -a v0.3.0 -m "Bulklink 0.3.0"
git push origin v0.3.0
```

Never rename, replace, or reuse an existing release tag or artifact. Every release is a
new immutable build.

## Required repository configuration

- configure a PyPI Trusted Publisher for repository `igors93/bulklink`;
- set the workflow name to `release.yml`;
- set the GitHub environment to `pypi`;
- protect the `pypi` environment with a required reviewer;
- do not add a `PYPI_TOKEN`, password, or username secret.

The release workflow grants `id-token: write` only to the publishing job. Build and test
jobs retain read-only repository permissions.

## Version and contract consistency

The version is present in both `pyproject.toml` and `bulklink.__version__`. Contract tests
and release verification fail when they differ or the changelog lacks a dated matching
section. If `BULKLINK_RELEASE_TAG` is present, it must equal `v{version}` exactly.

For the stable `0.3.x` line, the release also verifies the documented public exports,
enum values, immutable record fields, exception hierarchy, and primary calling
conventions. Intentional changes belong in a future minor release with changelog notes.

## Distribution verification

The release verifier checks:

- safe archive paths and absence of cache files;
- SPDX license metadata and the packaged `LICENSE` file;
- inclusion of the PEP 561 `py.typed` marker;
- installation of the wheel into a temporary virtual environment;
- runtime use of relative and absolute admission limits, resizing, diagnostics, events, shutdown, and the registry;
- the stable public contract from the installed wheel;
- strict type checking of a consumer against the installed wheel rather than `src/`.

The publishing job downloads and publishes the already verified files. It must never
rebuild artifacts.

## Future releases

Patch releases preserve the documented `0.3.x` contract. A future pre-1.0 minor release
may intentionally change or extend it, but must update contract tests, documentation, and
the changelog together.
