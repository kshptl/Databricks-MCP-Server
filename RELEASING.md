# Releasing

Releases go to PyPI through GitHub Actions with [trusted publishing](https://docs.pypi.org/trusted-publishers/), so no API token lives in the repo.

## One-time setup

1. Sign in at https://pypi.org (enable 2FA).
2. Go to **Your projects → Publishing → Add a new pending publisher** and enter:
   - PyPI project name: `databricks-mcp-toolkit`
   - Owner: `kshptl`
   - Repository name: `Databricks-MCP-Server`
   - Workflow name: `publish.yml`
   - Environment name: `pypi`
3. In the GitHub repo, go to **Settings → Environments → New environment** and name it `pypi`. Optionally require your approval before deploys.

## Each release

1. Bump `__version__` in `src/databricks_mcp/__init__.py` and add a `CHANGELOG.md` entry.
2. Merge to `main` and wait for CI to pass.
3. Create a GitHub release with the tag `v<version>`, for example `v0.2.0`. Publishing the release triggers `publish.yml`. It checks that the tag matches `__version__`, builds, runs `twine check`, and uploads.

## Manual fallback

If you'd rather publish from your machine:

```bash
pip install build twine
python -m build
twine check --strict dist/*
twine upload dist/*   # prompts for a PyPI API token
```

To dry-run on TestPyPI first, use `twine upload --repository testpypi dist/*`. Then install with `pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ databricks-mcp-toolkit`.
