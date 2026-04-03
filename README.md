# CDC PoC — Perl + DBIx::DataModel + Oracle Free

A **Change Data Capture (CDC)** proof-of-concept written in Perl, using
[DBIx::DataModel](https://metacpan.org/pod/DBIx::DataModel) as the ORM and
Oracle Free as the database, running in a container via
[gvenzl/oci-oracle-free](https://github.com/gvenzl/oci-oracle-free) (Podman + podman-compose).

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│  Docker Container  (gvenzl/oracle-free:slim-faststart)         │
│                                                                │
│  ┌──────────────────────┐    ┌───────────────────────────┐    │
│  │  Oracle Free DB       │    │  CDC Audit Schema         │    │
│  │  - DEPARTMENTS table  │◄──►│  - CDC_EVENTS table       │    │
│  │  - EMPLOYEES table    │    │  - DML triggers (auto)    │    │
│  └──────────────────────┘    └───────────────────────────┘    │
│               ▲                                                │
│               │ DBD::Oracle                                    │
│  ┌────────────┴──────────────────────────────────────────┐    │
│  │  Perl Application Layer                                │    │
│  │  ┌─────────────────────┐  ┌────────────────────────┐  │    │
│  │  │  App::Schema        │  │  CDC::Manager          │  │    │
│  │  │  (DBIx::DataModel)  │  │  (query / parse CDC)   │  │    │
│  │  └─────────────────────┘  └────────────────────────┘  │    │
│  │  ┌─────────────────────┐                              │    │
│  │  │  End-to-End Tests   │                              │    │
│  │  │  (Test::More, 20 ✓) │                              │    │
│  │  └─────────────────────┘                              │    │
│  └───────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────┘
```

### CDC Strategy

Row-level **Oracle DML triggers** are generated automatically by the
`create_cdc_trigger()` PL/SQL stored procedure and write before/after row
images to the `CDC_EVENTS` audit table.

Benefits:

- No Oracle LogMiner license required (works on Oracle Free).
- Events are transactional — they roll back together with the source DML.
- Transparent to application code — no ORM patches needed.

---

## Directory Layout

```
cdc-poc/
├── docker/
│   └── init/
│       ├── 01_schema.sql          # DEPARTMENTS + EMPLOYEES tables
│       ├── 02_cdc_schema.sql      # CDC_EVENTS + create_cdc_trigger()
│       └── 03_enable_cdc.sql      # Activates triggers on tracked tables
├── lib/
│   ├── App/Schema.pm              # DBIx::DataModel root schema
│   ├── App/Schema/Department.pm   # Department table class
│   ├── App/Schema/Employee.pm     # Employee table class
│   └── CDC/Manager.pm             # CDC query / parse helper
├── t/
│   └── 01_cdc_end_to_end.t        # 20-subtest end-to-end suite
├── cpanfile                       # Perl dependency manifest
├── docker-compose.yml
└── README.md
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Podman ≥ 4.9 | `sudo apt install podman` |
| podman-compose | `sudo apt install podman-compose` |
| Oracle Instant Client 23.6 (Basic + SDK ZIPs) | See step 1 below — required to compile `DBD::Oracle` |
| Perl ≥ 5.20 | System Perl or perlbrew |
| App::cpanminus | `sudo apt install cpanminus` or `cpan App::cpanminus` |
| alien, libaio1t64, libaio-dev | Only needed if using RPMs instead of ZIPs |

---

## Quick Start

### 1 — Install Oracle Instant Client (Basic + SDK)

`DBD::Oracle` must be compiled against the Oracle client headers and libraries.
The ZIP method works on any Linux distribution without root for the unzip step.

Download the two ZIP files from Oracle's website — no account required:
https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

Click **Version 23.6**, then download:
- **Basic Package** ZIP (`instantclient-basic-linux.x64-23.6.x.x.x.zip`)
- **SDK Package** ZIP (`instantclient-sdk-linux.x64-23.6.x.x.x.zip`)

Then install:

```bash
sudo mkdir -p /opt/oracle

# Adjust filenames to match what you downloaded
sudo unzip instantclient-basic-linux.x64-23.6.0.24.10.zip -d /opt/oracle
sudo unzip instantclient-sdk-linux.x64-23.6.0.24.10.zip   -d /opt/oracle

# Both ZIPs unzip into the same instantclient_23_6 directory
export ORACLE_HOME=/opt/oracle/instantclient_23_6
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH

# Persist across shells
echo 'export ORACLE_HOME=/opt/oracle/instantclient_23_6'       >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH'    >> ~/.bashrc
```

### 2 — Start Oracle

```bash
podman-compose up -d
```

Watch the logs until the init scripts finish and the database is ready
(slim-faststart typically takes under 90 s):

```bash
podman logs -f cdc-oracle
# Wait for: DATABASE IS READY TO USE!
# Then Ctrl+C
```

Or poll the healthcheck:

```bash
until podman exec cdc-oracle healthcheck.sh 2>/dev/null; do
    printf 'Waiting for Oracle…\n'; sleep 5
done
echo 'Oracle is ready.'
```

### 3 — Install Perl dependencies

First, set up `local::lib` so `cpanm` can install modules without root:

```bash
cpanm --local-lib=~/perl5 local::lib && eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)

# Persist across shells
echo 'eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)' >> ~/.bashrc
```

Then install all dependencies:

```bash
cpanm --installdeps .
```

> `ORACLE_HOME` and `LD_LIBRARY_PATH` must be set (step 1) before running this,
> otherwise `DBD::Oracle` will fail to compile.

### 4 — Set connection environment (optional)

The test script defaults to:

```
host=localhost  port=1521  service_name=FREEPDB1
user=appuser    pass=apppass
```

Override with:

```bash
export ORACLE_DSN="dbi:Oracle:host=localhost;port=1521;service_name=FREEPDB1"
export ORACLE_USER="appuser"
export ORACLE_PASS="apppass"
```

### 5 — Run the tests

```bash
prove -lv t/01_cdc_end_to_end.t
```

Or directly:

```bash
perl -Ilib t/01_cdc_end_to_end.t
```

### 6 — Tear down

```bash
podman-compose down -v
```

---

## Troubleshooting

**`short-name did not resolve to an alias`** — Podman requires fully-qualified image names.
The `docker-compose.yml` already uses `ghcr.io/gvenzl/oracle-free:slim-faststart` to avoid
this and Docker Hub rate limits.

**`oci.h: No such file or directory`** during `cpanm DBD::Oracle` — the SDK ZIP was not
unzipped, or `ORACLE_HOME` is not set. Verify with:
```bash
ls $ORACLE_HOME/sdk/include/oci.h
```

**Volume mount warnings (`"/" is not a shared mount`)** — harmless on rootless Podman;
the init SQL files are still mounted and executed correctly.

**SELinux / permission errors on the init volume** — append `:z` to the volume line in
`docker-compose.yml`:
```yaml
volumes:
  - ./docker/init:/container-entrypoint-initdb.d:z
```

---

## Test Coverage

| # | Subtest | What is verified |
|---|---------|-----------------|
| 1 | Infrastructure | DB connectivity, all tables and stored procedure present |
| 2 | Trigger installation | Both DML triggers compiled and active |
| 3 | INSERT (raw DBI) | Event captured, old_data NULL, new_data populated |
| 4 | INSERT (ORM) | DBIx::DataModel path, FK and default columns |
| 5 | UPDATE (raw DBI) | old/new image comparison, unchanged columns preserved |
| 6 | UPDATE (ORM) | DBIx::DataModel update path, salary change |
| 7 | DELETE | new_data NULL, old_data present, NAME correct |
| 8 | ROLLBACK | Zero events after rolled-back transaction |
| 9 | COMMIT | Multi-statement transaction: exactly N events |
| 10 | NULL values | NULL sentinel round-trips to undef |
| 11 | Bulk INSERT | One event per row in a loop |
| 12 | Bulk UPDATE | One event per affected row |
| 13 | MERGE – INSERT branch | Trigger fires on WHEN NOT MATCHED path |
| 14 | MERGE – UPDATE branch | Trigger fires on WHEN MATCHED path only |
| 15 | Constraint violation | Failed DML produces zero events |
| 16 | Cross-table FK | Parent + child INSERTs tracked; FK value preserved |
| 17 | Special characters | Accents, apostrophes, dashes round-trip correctly |
| 18 | Event metadata | event_id, event_time, table_name (upper-case) |
| 19 | event_pairs() | Helper returns paired old/new hashrefs for UPDATEs |
| 20 | TRUNCATE | DDL statement does not fire row-level triggers |

---

## Key Modules

### `CDC::Manager`

```perl
use CDC::Manager;
my $cdc = CDC::Manager->new(dbh => $dbh);

# Query events
my $events = $cdc->events_for(table => 'employees');
my $n      = $cdc->count_events(table => 'employees', operation => 'INSERT');
my $last   = $cdc->latest_event(table => 'employees', operation => 'UPDATE');

# Parse a row image
my $row = $cdc->parse_row_image($last->{new_data});
# { ID => '42', FIRST_NAME => 'Alice', SALARY => '90000', ACTIVE => '1', … }

# Maintenance
$cdc->clear_events();                        # wipe all events
$cdc->clear_events_for(table => 'employees');

# Convenience: UPDATE pairs
my $pairs = $cdc->event_pairs(table => 'employees');
# [ [$old_href, $new_href], … ]
```

### `App::Schema`

```perl
use App::Schema;
App::Schema->dbh($dbh);

my $dept = App::Schema->table('Department')->insert({ name => 'Eng', location => 'GVA' });
my $emp  = App::Schema->table('Employee')
               ->select(-where => { email => 'alice@example.com' })
               ->next;
$emp->update({ salary => 95_000 });
```

---

## Known Limitations

| Limitation | Notes |
|---|---|
| Serialisation format | Pipe-delimited `KEY=VALUE`; fragile if values contain `=` or `\|`. Use `JSON_OBJECT` in production. |
| DDL changes | Triggers must be regenerated after `ALTER TABLE`. Re-run `create_cdc_trigger`. |
| TRUNCATE | Oracle row-level triggers do not fire on `TRUNCATE`. Use DELETE for tracked tables. |
| No SCN | System Change Number not captured; add `ORA_ROWSCN` for strict cross-table ordering. |
| No LogMiner | DDL events not captured. LogMiner unavailable on Oracle Free. |
| Trigger overhead | Row triggers add per-row latency. Profile before using on high-throughput tables. |

---

## Future Extensions

- **JSON row images** — replace pipe-delimited strings with `JSON_OBJECT()`.
- **Hook-based CDC** — wrap `DBIx::DataModel` execution layer in Perl (portable, no DDL privileges needed).
- **Outbox / streaming** — poll `CDC_EVENTS` and publish to Kafka, RabbitMQ, or HTTP webhook.
- **SCN ordering** — correlate events to Oracle's global transaction order via `ORA_ROWSCN`.
- **DDL capture** — use Oracle Fine-Grained Auditing (FGA) or a system trigger to capture schema changes.

---

## References

- [gvenzl/oci-oracle-free](https://github.com/gvenzl/oci-oracle-free)
- [DBIx::DataModel on CPAN](https://metacpan.org/pod/DBIx::DataModel)
- [DBD::Oracle on CPAN](https://metacpan.org/pod/DBD::Oracle)
- [Oracle PL/SQL Triggers](https://docs.oracle.com/en/database/oracle/oracle-database/23/lnpls/plsql-triggers.html)
- [Oracle JSON_OBJECT](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_OBJECT.html)
