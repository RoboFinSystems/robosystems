# basedpyright: ignore

# Under `pytest -n`, each worker must be pointed at its own platform test
# database before anything imports `robosystems` (which binds its engine at
# import). This package init is the earliest hook that runs after pytest-env
# has applied the ini environment; see tests/xdist_workers.py.
from tests.xdist_workers import isolate_worker_databases

isolate_worker_databases()
