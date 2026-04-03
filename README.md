# DBIx::DataModel::Plugin::CDC

**Application-level Change Data Capture** for [DBIx::DataModel](https://metacpan.org/pod/DBIx::DataModel).

Captures INSERT, UPDATE, and DELETE events by extending the ORM's own
`table_parent` inheritance mechanism — no database triggers, stored
procedures, or DDL privileges required.  Pluggable handlers dispatch
events to a database table, message queue, webhook, or custom callback.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  DBIx::DataModel Schema                                         │
│                                                                 │
│  ┌─────────────────────────────────┐                            │
│  │  Plugin::CDC::Table             │  table_parent override     │
│  │  (extends Source::Table)        │  insert / update / delete  │
│  └────────────┬────────────────────┘                            │
│               │ dispatch                                        │
│  ┌────────────▼──────────────────────────────────────────────┐  │
│  │  Plugin::CDC  (registry + dispatcher)                     │  │
│  │                                                           │  │
│  │  in_transaction handlers ──► Handler::DBI (cdc_events)    │  │
│  │                           ──► Handler::Callback (custom)  │  │
│  │                                                           │  │
│  │  post_commit handlers    ──► Handler::Callback (queue)    │  │
│  │                           ──► Handler::Log (STDERR)       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Plugin::CDC::Event  ── builds canonical event envelope         │
│  Plugin::CDC::Handler::Multi  ── fan-out with error policies    │
└─────────────────────────────────────────────────────────────────┘
```

### How It Works

1. **`CDC::Table`** is set as `table_parent` at schema declaration time.
   Every table class inherits CDC-aware `insert`, `update`, and `delete`.

2. Before DML, the override snapshots the affected row(s).
   After DML, it builds an **event envelope** and dispatches to handlers.

3. **Transaction safety**: when `AutoCommit` is on, the DML + CDC write
   are wrapped in `do_transaction` (DBIx::DataModel's own mechanism).
   When `AutoCommit` is off, the caller's transaction governs both.

4. Handlers declare their **phase**:
   - `in_transaction` — runs inside the DB transaction (atomic with DML)
   - `post_commit` — runs after commit via `do_after_commit`

---

## Quick Start

### 1 — Prerequisites

| Requirement | Notes |
|---|---|
| Podman + podman-compose | `sudo apt install podman podman-compose` |
| Nothing else | Oracle client, Perl, and all deps run in containers |

### 2 — Run

```bash
./setup.sh
```

This builds the app container (Perl + Oracle Instant Client), starts
Oracle, waits for it, and runs the 43-test suite.

### 3 — Tear down

```bash
podman-compose down -v
```

---

## Usage

### Schema Declaration

```perl
use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC::Table;

DBIx::DataModel->Schema('App::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);

App::Schema->Table(Department => 'departments', 'id');
App::Schema->Table(Employee   => 'employees',   'id');
```

### CDC Setup

```perl
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Handler::DBI;
use DBIx::DataModel::Plugin::CDC::Handler::Callback;

App::Schema->dbh($dbh);

DBIx::DataModel::Plugin::CDC->setup('App::Schema',
    tables   => 'all',                    # or ['Department', 'Employee']
    handlers => [
        DBIx::DataModel::Plugin::CDC::Handler::DBI->new(
            table_name => 'cdc_events',   # writes JSON to this table
        ),
        DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
            phase    => 'post_commit',
            on_event => sub {
                my ($event) = @_;
                # Send to Redis, RabbitMQ, Kafka, webhook...
                $redis->xadd('cdc:events', '*',
                    payload => encode_json($event));
            },
        ),
    ],
);
```

### Query Events

```perl
my $CDC = 'DBIx::DataModel::Plugin::CDC';

my $events = $CDC->events_for('App::Schema',
    table => 'employees', operation => 'UPDATE');

my $last = $CDC->latest_event('App::Schema',
    table => 'employees', operation => 'INSERT');

my $n = $CDC->count_events('App::Schema', table => 'employees');

my $pairs = $CDC->event_pairs('App::Schema', table => 'employees');
# [ [\%old, \%new], ... ]

$CDC->clear_events('App::Schema');
$CDC->clear_events_for('App::Schema', table => 'employees');
```

---

## Event Envelope

Every handler receives a hashref with this structure:

```perl
{
    event_id        => '4a3f...',              # unique hex ID
    occurred_at     => '2026-04-03T14:32:01Z', # ISO 8601 UTC
    schema_name     => 'App::Schema',
    table_name      => 'EMPLOYEES',
    operation       => 'UPDATE',               # INSERT | UPDATE | DELETE
    old_data        => { ID => 42, SALARY => 75000, ... },  # undef for INSERT
    new_data        => { ID => 42, SALARY => 80000, ... },  # undef for DELETE
    changed_columns => ['SALARY'],             # UPDATE only, undef otherwise
}
```

The DBI handler serializes `old_data`/`new_data` to JSON.
Callback handlers receive raw Perl hashrefs.

---

## Handlers

### Handler::DBI

Writes events to a database table in the same transaction as the DML.

```perl
DBIx::DataModel::Plugin::CDC::Handler::DBI->new(
    table_name => 'cdc_events',    # default
);
```

- **Phase**: `in_transaction`
- **Serialization**: JSON via `Cpanel::JSON::XS`
- **Failure**: exception propagates → transaction rolls back

### Handler::Callback

Calls a user-provided coderef with the event envelope.

```perl
DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
    on_event => sub { my ($event, $schema) = @_; ... },
    phase    => 'post_commit',     # or 'in_transaction'
    on_error => 'warn',            # or 'abort', 'ignore'
);
```

### Handler::Log

Structured log to STDERR.  Useful for debugging.

```perl
DBIx::DataModel::Plugin::CDC::Handler::Log->new(
    prefix => 'CDC',               # default
);
```

- **Phase**: `post_commit`
- Output: `[CDC] EMPLOYEES UPDATE 4a3f...`

### Handler::Multi

Fan-out dispatcher with per-handler error policies.

```perl
DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
    handlers => [$dbi_handler, $callback_handler, $log_handler],
    on_error => 'warn',            # default policy
);
```

Each sub-handler can override the error policy via its `on_error` method.
Policies: `abort` (roll back DML), `warn` (log and continue), `ignore`.

---

## Transaction Safety

| Scenario | Behavior |
|---|---|
| `AutoCommit` ON | DML + CDC write wrapped in `do_transaction` — atomic |
| `AutoCommit` OFF | Caller's transaction governs both — commit/rollback together |
| `do_transaction` | Nested correctly — CDC hooks participate in the same txn |
| Constraint violation | DML fails → CDC write never happens |
| Handler failure (`abort`) | Exception propagates → transaction rolls back |
| Handler failure (`warn`) | Warning emitted → DML commits normally |
| `post_commit` handler | Runs after commit via `do_after_commit` — cannot rollback |

---

## Captured DML Paths

| ORM Path | Captured | Notes |
|---|---|---|
| `Table->insert({...})` | Yes | One event per record hashref |
| `$row->update({...})` | Yes | Snapshots old state from `$self` |
| `$row->delete()` | Yes | Snapshots old state before delete |
| `Table->update(-set => {}, -where => {})` | Yes | Pre-fetches affected rows, one event per row |
| `Table->delete(-where => {})` | Yes | Pre-fetches affected rows |
| `$dbh->do(...)` / raw SQL | **No** | By design — only ORM operations are captured |

---

## Performance

Benchmark on Oracle Free in a container (100 operations, `CDC_PERF_N=100`):

| Operation | Rate | Notes |
|---|---|---|
| Raw DBI INSERT (no CDC) | ~930 ops/s | Baseline |
| CDC ORM INSERT | ~230 ops/s | DML + JSON serialize + CDC table write |
| CDC ORM UPDATE | ~290 ops/s | Snapshot + DML + CDC write |
| CDC ORM DELETE | ~230 ops/s | Snapshot + DML + CDC write |
| Batch INSERT (txn) | ~370 ops/s | Single transaction, N inserts |

The overhead is dominated by the extra `INSERT INTO cdc_events` per operation.
For high-throughput tables, consider the **transactional outbox** pattern:
write to an outbox table in-transaction, relay to external systems asynchronously.

Set `CDC_PERF_N` to adjust benchmark size:

```bash
podman run --rm --network cdc-poc_default \
  -e ORACLE_DSN="dbi:Oracle:host=oracle;port=1521;service_name=FREEPDB1" \
  -e ORACLE_USER=appuser -e ORACLE_PASS=apppass \
  -e CDC_PERF_N=500 \
  cdc-poc-app
```

---

## Directory Layout

```
cdc-poc/
├── lib/
│   ├── DBIx/DataModel/Plugin/
│   │   ├── CDC.pm                    # Setup, config, dispatch, query helpers
│   │   └── CDC/
│   │       ├── Table.pm              # table_parent (insert/update/delete override)
│   │       ├── Event.pm              # Event envelope builder
│   │       └── Handler/
│   │           ├── DBI.pm            # JSON → cdc_events table
│   │           ├── Callback.pm       # User coderef
│   │           ├── Log.pm            # STDERR structured log
│   │           └── Multi.pm          # Fan-out with error policies
│   └── App/
│       ├── Schema.pm                 # DBIx::DataModel schema
│       └── Schema/
│           ├── Department.pm
│           └── Employee.pm
├── t/
│   └── 01_cdc_end_to_end.t          # 43 tests + performance benchmarks
├── docker/
│   ├── Dockerfile.app                # Perl + Oracle Instant Client
│   └── init/
│       ├── 01_schema.sh              # departments + employees tables
│       └── 02_cdc_schema.sh          # cdc_events table
├── docker-compose.yml
├── cpanfile
├── setup.sh                          # One-command setup + test
└── README.md
```

---

## Test Coverage

### CRUD (5 tests)
| # | Test | Verifies |
|---|------|----------|
| 1 | Infrastructure | DB connectivity, tables exist |
| 2 | INSERT Department | Event captured, old_data NULL, new_data JSON |
| 3 | INSERT Employee | FK value preserved in event |
| 4 | UPDATE instance | Old/new salary comparison |
| 5 | DELETE instance | old_data present, new_data NULL |

### Transactions (8 tests)
| # | Test | Verifies |
|---|------|----------|
| 6 | ROLLBACK | Zero events and zero rows |
| 7 | COMMIT multi-statement | Exact event count per operation |
| 8 | AutoCommit atomicity | DML + CDC event count match |
| 9 | Full lifecycle in txn | INSERT → UPDATE → DELETE |
| 10 | Partial rollback | Error mid-txn rolls back everything |
| 11 | Interleaved tables | Cross-table events in single txn |
| 12 | Constraint violation | No event for failed DML |
| 13 | Constraint in txn | Full rollback on duplicate |

### Class-Method Operations (3 tests)
| # | Test | Verifies |
|---|------|----------|
| 14 | Class-method UPDATE | Per-row old/new with full snapshot |
| 15 | Class-method DELETE | Per-row old_data capture |
| 16 | No matching rows | Zero events when WHERE matches nothing |

### Data Integrity (8 tests)
| # | Test | Verifies |
|---|------|----------|
| 17 | NULL columns | NULL preserved in JSON |
| 18 | Unchanged columns | old/new identical for unmodified fields |
| 19 | Bulk INSERT | One event per row |
| 20 | Cross-table FK | FK value in child event |
| 21 | Special characters | UTF-8 round-trip (accents, en-dash) |
| 22 | Empty string vs NULL | Oracle '' → NULL behavior |
| 23 | Multiple updates | Full history preserved |
| 24 | JSON round-trip | Valid JSON, correct structure |

### Metadata & Helpers (5 tests)
| # | Test | Verifies |
|---|------|----------|
| 25 | Event metadata | event_id, event_time, table_name |
| 26 | Event ordering | Ascending event_id |
| 27 | event_pairs() | Old/new hashref pairs for UPDATEs |
| 28 | count_events filter | Per-operation filtering |
| 29 | clear_events_for | Selective per-table cleanup |

### Plugin Features (9 tests)
| # | Test | Verifies |
|---|------|----------|
| 30 | Callback envelope | All fields present (event_id, occurred_at, schema_name) |
| 31 | changed_columns | Only modified columns listed |
| 32 | UPDATE old/new data | Hashrefs with correct values |
| 33 | INSERT/DELETE nulls | old_data/new_data undef where expected |
| 34 | Event::build IDs | Unique per call |
| 35 | changed_columns logic | Only for UPDATE, correct diff |
| 36 | Multi handler | Both DBI and Callback fire |
| 37 | Error policy: warn | DML succeeds, warning emitted |
| 38 | Error policy: abort | DML rolls back on handler failure |

### Performance (4 tests)
| # | Test | Verifies |
|---|------|----------|
| 39 | INSERT throughput | Raw DBI vs CDC ORM ops/s |
| 40 | UPDATE throughput | Per-row update rate |
| 41 | DELETE throughput | Per-row delete rate |
| 42 | Batch INSERT in txn | Transaction batching benefit |

### Design Trade-offs (1 test)
| # | Test | Verifies |
|---|------|----------|
| 43 | Raw DBI bypass | Confirms raw SQL is not captured |

---

## Known Limitations

| Limitation | Notes |
|---|---|
| Raw SQL bypass | Only ORM operations are captured. Direct `$dbh->do()` is invisible. |
| Pre-fetch overhead | Class-method `update`/`delete` fetch affected rows before DML. |
| No LogMiner | DDL events not captured. |
| Serialization | JSON via `Cpanel::JSON::XS`. Values containing non-UTF-8 binary may need custom handling. |
| Single schema | Registry is per-schema-class. Multi-schema setups need separate `setup()` calls. |

---

## Future Extensions

- **Transactional outbox** — write to outbox table in-txn, relay to Kafka/RabbitMQ/Redis asynchronously.
- **Handler::Redis** — Redis Streams (`XADD`) for lightweight event streaming.
- **Handler::AMQP** — RabbitMQ via `Net::AMQP::RabbitMQ` with publisher confirms.
- **Per-table handler config** — different handlers for different tables.
- **Schema-level filtering** — skip events for specific columns or operations.
- **CPAN publication** — package as `DBIx-DataModel-Plugin-CDC` distribution.

---

## References

- [DBIx::DataModel on CPAN](https://metacpan.org/pod/DBIx::DataModel)
- [DBD::Oracle on CPAN](https://metacpan.org/pod/DBD::Oracle)
- [Cpanel::JSON::XS on CPAN](https://metacpan.org/pod/Cpanel::JSON::XS)
- [gvenzl/oci-oracle-free](https://github.com/gvenzl/oracle-free)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)

---

## License

Copyright Yves. Apache 2.0.
