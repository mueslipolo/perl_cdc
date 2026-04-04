# DBIx::DataModel::Plugin::CDC

**Application-level Change Data Capture** for
[DBIx::DataModel](https://metacpan.org/pod/DBIx::DataModel).

Captures INSERT, UPDATE, and DELETE events by extending the ORM's own
`table_parent` inheritance mechanism.  No database triggers, stored
procedures, or DDL privileges required.  Listeners dispatch events to
a database table, message queue, webhook, or custom callback.

Database-agnostic.  Works with any DBI-supported backend.

## Installation

From CPAN:

```bash
cpanm DBIx::DataModel::Plugin::CDC
```

From source:

```bash
perl Makefile.PL && make && make test && make install
```

## Quick Start

```perl
use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Table;

# 1. Declare schema with CDC table_parent
DBIx::DataModel->Schema('App::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);
App::Schema->Table(Department => 'departments', 'id');
App::Schema->Table(Employee   => 'employees',   'id');

# 2. Connect and configure CDC
App::Schema->dbh($dbh);

DBIx::DataModel::Plugin::CDC
    ->setup('App::Schema', tables => 'all')
    ->log_to_dbi('App::Schema', 'cdc_events')
    ->on('App::Schema', '*' => sub {
        my ($event, $schema) = @_;
        # push to Redis, webhook, etc.
    });

# 3. Use the ORM normally — events are captured automatically
App::Schema->table('Department')->insert({ name => 'Engineering' });
```

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
6. The event is **dispatched to listeners**:
   - `in_transaction` listeners (e.g., `log_to_dbi`) run inside the DB transaction.
   - `post_commit` listeners (e.g., custom `->on()` callbacks) run after commit.
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

### Multi-Table Operations

CDC tracks each table independently.  When you insert a parent row then
a child row, each generates its own event.  In a transaction that touches
multiple tables, all events are captured and commit or roll back together.

For schemas using `Composition` (subtree inserts, cascaded deletes),
the child operations internally call `insert()` / `delete()` on the
child table class — which is hooked.  All subtree operations are captured.

### Transaction Safety

All CDC operations are **atomic with the DML**:

| Scenario | Behavior |
|---|---|
| `AutoCommit` ON | Mini-transaction wraps DML + CDC event |
| `AutoCommit` OFF | Your transaction governs both |
| Inside `do_transaction` | Post-commit listeners deferred via `do_after_commit` |
| DML fails (constraint violation) | CDC event never written |
| `in_transaction` listener fails (`abort` policy) | DML rolled back |
| `post_commit` listener fails | DML already committed — data safe |

There is **no window** where a DML is committed without its CDC event (for
`in_transaction` listeners).  Both are in the same database transaction.

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
        │     └─ CDC::dispatch()           ← routes to listeners
        │           │
        │           ├─ log_to_dbi          (in_transaction → JSON to DB)
        │           └─ ->on() callbacks    (post_commit → your code)
        │
        └─ commit (or rollback on error)
```

### Module Structure

```
DBIx::DataModel::Plugin::
├── CDC.pm         setup(), on(), log_to_dbi(), log_to_stderr(),
│                  dispatch, query helpers
├── CDC/Table.pm   table_parent — overrides insert/update/delete
└── CDC/Event.pm   event envelope builder (ID, timestamp, diff)
```

Three modules.  That's it.

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

### 2. Configure Listeners

```perl
use DBIx::DataModel::Plugin::CDC;

App::Schema->dbh($dbh);

DBIx::DataModel::Plugin::CDC
    ->setup('App::Schema', tables => 'all')    # or tables => ['Department']

    # Built-in: persist events as JSON to a DB table (in_transaction, abort on error)
    ->log_to_dbi('App::Schema', 'cdc_events')

    # Built-in: one-line structured log to STDERR (post_commit)
    ->log_to_stderr('App::Schema')

    # Custom: your code, any operation
    ->on('App::Schema', '*' => sub {
        my ($event, $schema) = @_;
        # push to Redis, webhook, Kafka...
    })

    # Custom: only inserts, inside the transaction, abort on failure
    ->on('App::Schema', 'INSERT' => sub {
        my ($event, $schema) = @_;
        # critical audit check
    }, { phase => 'in_transaction', on_error => 'abort' });
```

All methods return `$class` for chaining.

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

Requires `log_to_dbi()` to have been configured.

```perl
my $CDC = 'DBIx::DataModel::Plugin::CDC';

my $events = $CDC->events_for('App::Schema',
    table => 'employees', operation => 'UPDATE');

my $last = $CDC->latest_event('App::Schema',
    table => 'employees', operation => 'INSERT');

my $n = $CDC->count_events('App::Schema', table => 'employees');

# UPDATE pairs (old/new as decoded hashrefs)
my $pairs = $CDC->event_pairs('App::Schema', table => 'employees');
# [ [\%old, \%new], ... ]

$CDC->clear_events('App::Schema');
$CDC->clear_events_for('App::Schema', table => 'employees');
```

---

## Event Envelope

Every listener receives an event hashref with this structure:

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

`log_to_dbi` serializes `old_data`/`new_data` to JSON.  Custom `->on()`
listeners receive the raw Perl hashrefs.

Event IDs are time-based and monotonically increasing within a process:
`<seconds>-<microseconds>-<pid>-<counter>`.

---

## Listener API

### `->on($schema, $operation, \&callback, \%opts?)`

Register a listener.  `$operation` is `'INSERT'`, `'UPDATE'`, `'DELETE'`,
or `'*'` (all operations).

```perl
$CDC->on('App::Schema', '*' => sub {
    my ($event, $schema) = @_;
    # ...
}, {
    phase    => 'post_commit',     # default; or 'in_transaction'
    on_error => 'warn',            # default; or 'abort', 'ignore'
});
```

**Phases:**

| Phase | When | Has DB access? | Can rollback DML? |
|---|---|---|---|
| `in_transaction` | Before commit | Yes, same `$dbh` | Yes, on failure |
| `post_commit` | After commit | No | No |

**Error policies:**

| Policy | Effect |
|---|---|
| `abort` | Exception propagates → transaction rolls back → DML cancelled |
| `warn` | Warning emitted → DML commits normally |
| `ignore` | Silently suppressed (logs with `CDC_DEBUG=1` env var) |

### `->log_to_dbi($schema, $table_name?)`

Built-in listener: persist events as JSON to a database table.  Defaults
to `'cdc_events'`.  Runs `in_transaction` with `abort` on error.

Table name is validated against SQL injection (`/\A[a-zA-Z_]\w*\z/`).
Uses a prepared statement cache for performance.

### `->log_to_stderr($schema, $prefix?)`

Built-in listener: print `[CDC] TABLE OPERATION event_id` to STDERR.
Defaults prefix to `'CDC'`.  Runs `post_commit` with `ignore` on error.

### Writing a Custom Listener

A listener is just a coderef that accepts `($event, $schema)`:

```perl
# Send to Redis Streams
$CDC->on('App::Schema', '*' => sub {
    my ($event, $schema) = @_;
    $redis->xadd('cdc:events', '*',
        table     => $event->{table_name},
        operation => $event->{operation},
        payload   => encode_json($event),
    );
}, { phase => 'post_commit' });

# Wrap an object method
my $auditor = My::Auditor->new;
$CDC->on('App::Schema', '*' => sub { $auditor->handle(@_) },
    { phase => 'in_transaction', on_error => 'abort' });
```

No base class to inherit.  No interface to implement.  Just a sub.

---

## Captured DML Paths

| ORM Path | Captured | Notes |
|---|---|---|
| `Table->insert({...})` | Yes | One event per record |
| `$row->update({...})` | Yes | Snapshots old state from loaded row |
| `$row->delete()` | Yes | Snapshots old state before deletion |
| `Table->update(-set => {}, -where => {})` | Yes | Pre-fetches affected rows inside txn |
| `Table->delete(-where => {})` | Yes | Pre-fetches affected rows inside txn |
| Composition subtree insert | Yes | Child `insert()` is hooked |
| Composition cascaded delete | Yes | Child `delete()` is hooked |
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
round-trip per DML.  Perl-side costs (JSON serialization, event envelope)
are negligible.

Optimizations applied:
- Prepared statement cache — one `prepare` per `$dbh`
- Lightweight mini-transaction — avoids `do_transaction` machinery
- Cached timestamps — `gmtime` at most once per second
- Time-based event IDs — monotonic, zero `rand()` calls

---

## Distribution Layout

```
DBIx-DataModel-Plugin-CDC/
├── lib/
│   └── DBIx/DataModel/Plugin/
│       ├── CDC.pm                    # setup, on, log_to_dbi, dispatch, queries
│       └── CDC/
│           ├── Table.pm              # table_parent (insert/update/delete)
│           └── Event.pm              # Event envelope builder
├── t/                                # Unit tests (no database required)
│   ├── 00_compile.t                  # All modules load
│   ├── 01_event.t                    # Event envelope, IDs, validation
│   ├── 02_handler_base.t            # on(), log_to_dbi, log_to_stderr validation
│   ├── 03_handler_multi.t           # Dispatch, operation filtering, error policies
│   └── 04_setup.t                    # Registry, selective tables
├── examples/
│   └── oracle-cdc-poc/               # Integration example (Oracle)
│       ├── setup.sh                  # One-command build + test
│       ├── docker-compose.yml
│       ├── docker/
│       ├── lib/App/
│       └── t/01_cdc_end_to_end.t     # 49 integration tests + benchmarks
├── Makefile.PL
├── cpanfile
├── Changes
├── LICENSE                           # Apache 2.0
├── MANIFEST
└── README.md
```

## Test Coverage

**74 total tests**: 25 unit + 49 integration.

### Unit Tests (t/ — no database, runs on CPAN smoke)

| File | Tests | Covers |
|---|---|---|
| `00_compile.t` | 3 | All 3 modules load cleanly |
| `01_event.t` | 18 | Event::build, unique IDs (1000), ISO 8601 timestamps, changed_columns, validation |
| `02_handler_base.t` | 16 | `on()` validation (phase, on_error, coderef), `log_to_dbi` SQL injection rejection, `log_to_stderr`, chaining |
| `03_handler_multi.t` | 10 | Operation filtering, wildcard, listener ordering, abort/warn/ignore error policies, abort propagation |
| `04_setup.t` | 7 | Registry with `tables => 'all'` and selective, `is_tracked`, validation |

### Integration Tests (examples/oracle-cdc-poc/t/ — needs Oracle)

| Section | Tests | Covers |
|---|---|---|
| CRUD | 5 | INSERT, UPDATE, DELETE with event content verification |
| Transactions | 8 | ROLLBACK, COMMIT, AutoCommit atomicity, lifecycle, interleaved tables, constraint violations |
| Class-method ops | 3 | Bulk UPDATE/DELETE per-row, no-match zero events |
| Data integrity | 8 | NULL, unchanged columns, bulk, FK, UTF-8, empty string, update history, JSON round-trip |
| Metadata & helpers | 5 | event_id, ordering, event_pairs, count filter, selective clear |
| Plugin features | 10 | Callback envelope, changed_columns, multiple listeners, abort/warn policies, log_to_stderr |
| Relationships | 5 | FK parent/child, cross-table rollback, snapshot filters refs, rapid multi-table ops |
| Performance | 4 | INSERT/UPDATE/DELETE throughput (ORM vs ORM+CDC), batch in txn |
| Trade-offs | 1 | Raw DBI bypass confirmation |

---

## Design Philosophy

- **Event emitter pattern** — listeners are coderefs, not classes.  `->on()` is the only API.
- **Built-in shortcuts** — `log_to_dbi()` and `log_to_stderr()` cover the common cases without any user code.
- **Three modules total** — `CDC.pm`, `Table.pm`, `Event.pm`.  No handler classes, no abstract base, no Multi dispatcher.
- **`namespace::clean`** on all modules.
- **`croak`** for errors (string exceptions).
- **Zero framework dependencies** beyond what DBIx::DataModel already uses.

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
- **Per-table listener config** — different listeners for different tables
- **Column filtering** — skip specific columns from CDC capture
- **Operation-specific `log_to_dbi`** — only persist certain operations

---

## References

- [DBIx::DataModel on CPAN](https://metacpan.org/pod/DBIx::DataModel)
- [Cpanel::JSON::XS on CPAN](https://metacpan.org/pod/Cpanel::JSON::XS)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)

---

## License

Copyright Yves. Apache 2.0.
