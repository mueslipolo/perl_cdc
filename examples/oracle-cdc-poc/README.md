# Oracle CDC PoC

Example application demonstrating `DBIx::DataModel::Plugin::CDC` with
Oracle Free in a container.

## Quick Start

```bash
./setup.sh
```

Requires only `podman` and `podman-compose`. Everything else runs in containers.

## What It Does

- Departments + Employees schema with FK
- CDC plugin captures all ORM operations to `cdc_events` table
- 51 integration tests + performance benchmarks
- Set `CDC_PERF_N=500` to adjust benchmark size

## Tear Down

```bash
podman-compose down -v
```
