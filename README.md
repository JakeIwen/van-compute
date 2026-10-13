# van-compute

`van-compute` owns the coupled Pi broker and Mac worker for guarded offline compute jobs. Deployment source paths intentionally retain their historical repository-relative layout because installed and frozen releases fingerprint those paths.

## Test

From the repository root, using a Python 3.11+ environment with Flask installed:

```zsh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s pi/tests/compute
```

The suite is self-contained and does not require a sibling `scripts` checkout.

## Deploy

Read [`pi/docs/compute/VAN_COMPUTE.md`](pi/docs/compute/VAN_COMPUTE.md) before deploying. Review the local-only plan first:

```zsh
./macbook/scripts/install_van_compute_worker.zsh --dry-run
```

Run a live install only from the owner's regular Terminal after reviewing the checkout:

```zsh
./macbook/scripts/install_van_compute_worker.zsh
```

Routine monorepo updates continue through `/Users/jacobr/dev/scripts/pi/sync_scripts.sh`, which invokes this checkout with `--if-needed` after its package updates.
