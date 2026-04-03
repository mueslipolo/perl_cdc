# DBIx::DataModel::Plugin::CDC

**Application-level Change Data Capture** for
[DBIx::DataModel](https://metacpan.org/pod/DBIx::DataModel).

Captures INSERT, UPDATE, and DELETE events by extending the ORM's own
`table_parent` inheritance mechanism.  No database triggers, stored
procedures, or DDL privileges required.  Pluggable handlers dispatch
events to a database table, message queue, webhook, or custom callback.

---

## How It Works

### The Core Idea

DBIx::DataModel lets you specify a `table_parent` class that every table
inherits from.  This plugin provides `CDC::Table` — a subclass of the
default `Source::Table` that **overrides `insert`, `update`, and `delete`**
to capture change events before delegating to the original method.

Your application code does not change.  The CDC capture is transparent.

### What Happens When You Insert a Row

```perl
App::Schema->table('Department')->insert({ name => 'Engineering' });
```

1. **`CDC::Table::insert()`** is called (Department inherits from it).
2. It checks if this table is tracked for CDC.  If not, it passes through
   to the original `insert()` with zero overhead.
3. If tracked, it wraps the operation in a **mini-transaction** to ensure
   the DML and CDC event are atomic — both commit or both roll back.
4. The **original `insert()`** executes — normal SQL, normal result.
5. An **event envelope** is built with a unique ID, timestamp, table name,
   operation type, and the new row data as a Perl hashref.
6. The event is **dispatched to handlers**:
   - `in_transaction` handlers (e.g., DBI) run inside the DB transaction.
   - `post_commit` handlers (e.g., Callback) run after commit.
7. The mini-transaction **commits**.  Both the row and the CDC event are durable.

### UPDATE and DELETE

For **UPDATE**, the plugin snapshots the row's current state before the
change, runs the update, then records both old and new data.  For
`$row->update({salary => 80_000})`, the old salary and new salary are
both captured.

For **DELETE**, it snapshots the row before deletion.  The event contains
`old_data` with the full row and `new_data => undef`.

**Class-method operations** (`Table->update(-set => {...}, -where => {...})`)
are handled by pre-fetching the affected rows inside the same transaction,
running the DML, then generating one CDC event per affected row.

### Transaction Safety

All CDC operations are **atomic with the DML**:

| Scenario | Behavior |
|---|---|
| `AutoCommit` ON | Mini-transaction wraps DML + CDC event |
| `AutoCommit` OFF | Your transaction governs both |
| Inside `do_transaction` | Post-commit handlers deferred via `do_after_commit` |
| DML fails (constraint violation) | CDC event never written |
| `in_transaction` handler fails (`abort` policy) | DML rolled back |
| `post_commit` handler fails | DML already committed — data safe |

There is **no window** where a DML is committed without its CDC event (for
`in_transaction` handlers).  Both are in the same database transaction.

---

## Architecture

```
  Your code                      Plugin internals
  ─────────                      ────────────────
  Table->insert({...})
        │
        ▼
  CDC::Table::insert()           ← overrides Source::Table::insert
        │
        ├─ is table tracked? ──no──► next::method (pass-through)
        │
        ├─ _cdc_ensure_atomic    ← wraps in mini-txn if needed
        │     │
        │     ├─ Source::Table::insert()   ← the real INSERT
        │     │
        │     ├─ Event::build()            ← event envelope
        │     │
        │     └─ CDC::dispatch()           ← routes to handlers
        │           │
        │           ├─ Handler::DBI        (in_transaction → JSON to cdc_events)
        │           ├─ Handler::Callback   (post_commit → your coderef)
        │           └─ Handler::Log        (post_commit → STDERR)
        │
        └─ commit (or rollback on error)
```

### Module Structure

```
DBIx::DataModel::Plugin::CDC
├── CDC.pm              Registry, dispatch, query helpers
├── CDC/Table.pm        table_parent class — overrides insert/update/delete
├── CDC/Event.pm        Builds event envelopes (ID, timestamp, diff)
├── CDC/Handler.pm      Abstract base class (enforces interface contract)
└── CDC/Handler/
    ├── DBI.pm          Writes JSON to a database table (in_transaction)
    ├── Callback.pm     Calls user coderef (configurable phase)
    ├── Log.pm          Prints to STDERR (post_commit, for debugging)
    └── Multi.pm        Combines handlers with per-handler error policies
```

---

## Quick Start

### Prerequisites

Only `podman` and `podman-compose` are needed.  Everything else
(Oracle, Perl, CPAN modules) runs in containers.

### Run

```bash
./setup.sh
```

Builds the app container, starts Oracle, waits for it, runs 44 tests.

### Tear Down

```bash
podman-compose down -v
```

---

## Usage

### 1. Declare the Schema

```perl
use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC::Table;

DBIx::DataModel->Schema('App::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);

App::Schema->Table(Department => 'departments', 'id');
App::Schema->Table(Employee   => 'employees',   'id');
```

The `table_parent` line is the only change to your schema declaration.
All table classes now inherit CDC-aware `insert`, `update`, and `delete`.

### 2. Configure Handlers

```perl
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Handler::DBI;
use DBIx::DataModel::Plugin::CDC::Handler::Callback;

App::Schema->dbh($dbh);

DBIx::DataModel::Plugin::CDC->setup('App::Schema',
    tables   => 'all',                    # or ['Department']
    handlers => [
        # Write events to the cdc_events table (same transaction)
        DBIx::DataModel::Plugin::CDC::Handler::DBI->new(
            table_name => 'cdc_events',
        ),

        # Custom logic after commit (e.g., push to Redis)
        DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
            phase    => 'post_commit',
            on_event => sub {
                my ($event, $schema) = @_;
                # $event is a hashref — see "Event Envelope" below
            },
        ),
    ],
);
```

### 3. Use the ORM Normally

```perl
# These all generate CDC events automatically:
App::Schema->table('Department')->insert({ name => 'Eng', location => 'GVA' });

my $dept = (App::Schema->table('Department')
    ->select(-where => { name => 'Eng' }))->[0];
$dept->update({ location => 'ZRH' });
$dept->delete();

# Class-method bulk update — one event per affected row:
App::Schema->table('Employee')->update(
    -set   => { salary => 90_000 },
    -where => { department_id => 42 },
);
```

### 4. Query Events

```perl
my $CDC = 'DBIx::DataModel::Plugin::CDC';

# All events for a table
my $events = $CDC->events_for('App::Schema',
    table => 'employees', operation => 'UPDATE');

# Latest event
my $last = $CDC->latest_event('App::Schema',
    table => 'employees', operation => 'INSERT');

# Count
my $n = $CDC->count_events('App::Schema', table => 'employees');

# UPDATE pairs (old/new as decoded hashrefs)
my $pairs = $CDC->event_pairs('App::Schema', table => 'employees');
# [ [\%old, \%new], [\%old, \%new], ... ]

# Cleanup
$CDC->clear_events('App::Schema');
$CDC->clear_events_for('App::Schema', table => 'employees');
```

---

## Event Envelope

Every handler receives an event hashref with this structure:

```perl
{
    event_id        => '680e3a1f-0a2b-1a3c-0001',
    occurred_at     => '2026-04-03T14:32:01Z',       # ISO 8601 UTC
    schema_name     => 'App::Schema',
    table_name      => 'EMPLOYEES',                   # always upper-case
    operation       => 'UPDATE',                      # INSERT | UPDATE | DELETE

    old_data        => { ID => 42, SALARY => 75000, FIRST_NAME => 'Alice', ... },
    new_data        => { ID => 42, SALARY => 80000, FIRST_NAME => 'Alice', ... },
    changed_columns => ['SALARY'],                    # UPDATE only
}
```

| Field | INSERT | UPDATE | DELETE |
|---|---|---|---|
| `old_data` | `undef` | hashref (before) | hashref (before) |
| `new_data` | hashref (after) | hashref (after) | `undef` |
| `changed_columns` | `undef` | arrayref | `undef` |

The DBI handler serializes `old_data`/`new_data` to JSON when writing to
the database.  Callback handlers receive the raw Perl hashrefs.

Event IDs are time-based and monotonically increasing within a process:
`<seconds>-<microseconds>-<pid>-<counter>`.

---

## Handlers

### Handler::DBI — Database Persistence

Writes each event as a row in a database table, inside the same transaction
as the DML.  This is the primary audit trail.

```perl
DBIx::DataModel::Plugin::CDC::Handler::DBI->new(
    table_name => 'cdc_events',    # default; validated against SQL injection
);
```

- **Phase**: `in_transaction` — atomic with the DML
- **Serialization**: JSON via `Cpanel::JSON::XS`
- **Performance**: uses a prepared statement cache (one `prepare` per `$dbh`)
- **Failure**: exception propagates → transaction rolls back (DML + event)

### Handler::Callback — Custom Logic

Calls a user-provided coderef for each event.

```perl
DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
    on_event => sub {
        my ($event, $schema) = @_;
        # $event is the hashref shown above
        # $schema is the DBIx::DataModel schema object
    },
    phase    => 'post_commit',     # or 'in_transaction'
    on_error => 'warn',            # or 'abort', 'ignore'
);
```

Use `in_transaction` if you need database access via `$schema->dbh`.
Use `post_commit` for external systems (queues, webhooks) — a failure
won't roll back the DML.

### Handler::Log — Debugging

Prints a structured one-line log to STDERR.

```perl
DBIx::DataModel::Plugin::CDC::Handler::Log->new(
    prefix => 'CDC',               # default
);
# Output: [CDC] DEPARTMENTS INSERT 680e3a1f-0a2b-1a3c-0001
```

- **Phase**: `post_commit`

### Handler::Multi — Fan-Out

Combines multiple handlers with error isolation and per-handler policies.

```perl
DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
    handlers => [
        DBIx::DataModel::Plugin::CDC::Handler::DBI->new(...),
        DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
            on_event => sub { ... },
            phase    => 'post_commit',
            on_error => 'warn',       # per-handler policy
        ),
    ],
    on_error => 'warn',               # fallback policy
);
```

**Error policies:**

| Policy | Effect |
|---|---|
| `abort` | Exception propagates → transaction rolls back → DML cancelled |
| `warn` | Warning emitted → DML commits normally |
| `ignore` | Silently suppressed (logs with `CDC_DEBUG=1` env var) |

Use this when the database audit trail is critical (`abort`) but a
Redis notification is best-effort (`warn`).

### Writing Your Own Handler

Inherit from `Handler` and implement two methods:

```perl
package My::Handler::Webhook;
use parent 'DBIx::DataModel::Plugin::CDC::Handler';

sub phase { 'post_commit' }

sub dispatch_event {
    my ($self, $event, $schema) = @_;
    # POST $event to a webhook, write to Kafka, etc.
}
```

The base class enforces the contract: if you forget to implement
`dispatch_event` or `phase`, you get a clear error message.

---

## Captured DML Paths

| ORM Path | Captured | Notes |
|---|---|---|
| `Table->insert({...})` | Yes | One event per record |
| `$row->update({...})` | Yes | Snapshots old state from loaded row |
| `$row->delete()` | Yes | Snapshots old state before deletion |
| `Table->update(-set => {}, -where => {})` | Yes | Pre-fetches affected rows |
| `Table->delete(-where => {})` | Yes | Pre-fetches affected rows |
| `$dbh->do(...)` / raw SQL | **No** | By design — only ORM operations |

---

## Performance

Comparing DBIx::DataModel **without CDC** vs **with CDC** (Oracle Free
container, N=100):

| Operation | ORM only | ORM + CDC | CDC overhead |
|---|---|---|---|
| INSERT | ~400 ops/s | ~290 ops/s | +37% |
| UPDATE | ~500 ops/s | ~310 ops/s | +59% |
| DELETE | ~480 ops/s | ~260 ops/s | +82% |
| Batch INSERT (txn) | ~1000 ops/s | ~460 ops/s | +117% |

The overhead is dominated by the extra `INSERT INTO cdc_events` database
round-trip per DML operation.  Perl-side costs (JSON serialization, event
envelope) are negligible.

Optimizations applied:
- Prepared statement cache — `INSERT` is prepared once per `$dbh`, reused
- Lightweight mini-transaction — avoids `do_transaction` overhead
- Cached timestamps — `gmtime` called at most once per second
- Time-based event IDs — monotonic, no `rand()` calls

Tune benchmark size with `CDC_PERF_N`:

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
│   │   ├── CDC.pm                    # Registry, dispatch, query helpers
│   │   └── CDC/
│   │       ├── Table.pm              # table_parent (insert/update/delete)
│   │       ├── Event.pm              # Event envelope builder
│   │       ├── Handler.pm            # Abstract base class
│   │       └── Handler/
│   │           ├── DBI.pm            # JSON → cdc_events table
│   │           ├── Callback.pm       # User coderef
│   │           ├── Log.pm            # STDERR structured log
│   │           └── Multi.pm          # Fan-out + error policies
│   └── App/
│       ├── Schema.pm                 # DBIx::DataModel schema
│       └── Schema/
│           ├── Department.pm
│           └── Employee.pm
├── t/
│   └── 01_cdc_end_to_end.t          # 44 tests + performance benchmarks
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

## Test Coverage (44 tests)

### CRUD (5 tests)
INSERT Department, INSERT Employee with FK, UPDATE instance method,
DELETE instance method — verifies event content, old/new data, operations.

### Transactions (8 tests)
ROLLBACK, COMMIT multi-statement, AutoCommit atomicity, full lifecycle
(INSERT→UPDATE→DELETE in single txn), partial rollback on error,
interleaved tables, constraint violations in and out of transactions.

### Class-Method Operations (3 tests)
Bulk UPDATE with per-row events, bulk DELETE with per-row old_data,
no-match UPDATE produces zero events.

### Data Integrity (8 tests)
NULL columns, unchanged columns preserved, bulk INSERT (5→5 events),
cross-table FK, special characters (UTF-8), empty string vs NULL,
multiple updates on same row (full history), JSON round-trip.

### Metadata & Helpers (5 tests)
Event metadata fields, event ordering, event_pairs helper,
count_events filtering, selective clear_events_for.

### Plugin Features (10 tests)
Callback envelope structure, changed_columns for UPDATE, old/new data
in callbacks, INSERT/DELETE null semantics, Event::build unique IDs,
changed_columns logic, Multi handler dispatch, error policy warn
(DML commits), error policy abort (DML rolls back), Handler::Log output.

### Performance (4 tests)
INSERT/UPDATE/DELETE throughput (ORM vs ORM+CDC), batch INSERT in
transaction.  Reports ops/s and overhead percentage.

### Design Trade-offs (1 test)
Raw DBI bypass — confirms raw SQL is not captured (by design).

---

## Design Philosophy

This plugin follows DBIx::DataModel's own coding conventions:

- **Raw `bless`**, not Moo/Moose — table rows are plain hashrefs
- **`Params::Validate`** for constructor argument checking
- **`namespace::clean`** on all modules
- **`croak`** for errors (string exceptions, not objects)
- **Abstract base class** (`Handler.pm`) using `define_abstract_methods`
  from `DBIx::DataModel::Meta::Utils`
- **Zero framework dependencies** beyond what DBIx::DataModel already uses

---

## Known Limitations

| Limitation | Notes |
|---|---|
| Raw SQL bypass | `$dbh->do(...)` is invisible to the plugin |
| Pre-fetch overhead | Class-method `update`/`delete` runs a SELECT first |
| Single-process IDs | Event IDs are unique per process; use the DB-generated `event_id` column for global ordering |
| JSON in CLOB | Wide tables produce large JSON payloads in Oracle CLOB columns |

---

## Future Extensions

- **Transactional outbox** — buffer events, relay to Kafka/RabbitMQ asynchronously
- **Handler::Redis** — Redis Streams (`XADD`) for lightweight event streaming
- **Handler::AMQP** — RabbitMQ with publisher confirms
- **Per-table handler config** — different handlers for different tables
- **Column filtering** — skip specific columns from CDC capture
- **CPAN publication** — package as `DBIx-DataModel-Plugin-CDC` distribution

---

## References

- [DBIx::DataModel on CPAN](https://metacpan.org/pod/DBIx::DataModel)
- [DBD::Oracle on CPAN](https://metacpan.org/pod/DBD::Oracle)
- [Cpanel::JSON::XS on CPAN](https://metacpan.org/pod/Cpanel::JSON::XS)
- [Params::Validate on CPAN](https://metacpan.org/pod/Params::Validate)
- [gvenzl/oci-oracle-free](https://github.com/gvenzl/oracle-free)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)

---

## License

Copyright Yves. Apache 2.0.
