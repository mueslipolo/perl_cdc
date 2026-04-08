# Oracle CDC PoC

Example application demonstrating `DBIx::DataModel::Plugin::CDC` with
Oracle Free in a container.

## Quick Start

```bash
./setup.sh
```

Requires `podman` and `podman-compose` (or `docker` and `docker-compose`).
Everything else runs in containers.

## What It Does

Runs the **shared CDC test suite** (`t/lib/CDCTestSuite.pm`) against a
real Oracle database, plus Oracle-specific tests that cannot run on SQLite:

| Category | What it tests |
|----------|--------------|
| Shared suite | 30 subtests: CRUD, class-method ops, atomicity, capture_old, query helpers, composition, edge cases |
| Transactions | ROLLBACK, COMMIT, constraint violations, cross-table rollback |
| Oracle semantics | Empty string = NULL, special character round-trip, snapshot filters |
| Performance | INSERT/UPDATE/DELETE benchmarks: ORM vs ORM+CDC (set `CDC_PERF_N=500` to adjust) |
| Design trade-offs | Raw DBI bypass not captured (by design) |

## Tear Down

```bash
podman-compose down -v
```
