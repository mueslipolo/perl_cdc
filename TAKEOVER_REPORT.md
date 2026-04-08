# Takeover Report — DBIx::DataModel::Plugin::CDC

*Perspective: mid-level Perl developer, first day on the project.*
*Date: 2026-04-08*

---

## 1. DOCUMENTATION

### 1.1 README.md — Very strong, some gaps

The README is thorough and well-structured. Architecture diagram, event envelope reference, listener API, performance numbers, known limitations — it's all there. As a new developer, I could understand *what the project does* and *how to use it* within 15 minutes.

**Pain points:**

- **No "Getting Started for Contributors" section.** The README covers `cpanm DBIx::DataModel::Plugin::CDC` (consumer install) and `perl Makefile.PL && make && make test` (from source), but doesn't mention the `local/` lib setup needed to actually develop. I ran `make test` and it failed because deps weren't installed. Had to figure out `cpanm --local-lib=./local --installdeps .` and `PERL5LIB=./local/lib/perl5:./lib prove -lv t/` on my own. A `CONTRIBUTING.md` or a "Development" section would save time.

- **The `cdc_events` table DDL is only in `examples/oracle-cdc-poc/docker/init/02_cdc_schema.sh`.** If I use `log_to_dbi()`, what columns does the table need? The README says `log_to_dbi` persists "events as JSON" but the actual INSERT uses `(table_name, operation, old_data, new_data)` — four columns. The Oracle DDL has eight columns (`event_id`, `event_time`, `session_user`, `transaction_id` etc.), most of which are DB-generated and not written by the plugin. This mismatch is confusing. There should be a minimal "generic SQL" DDL in the README showing exactly what `log_to_dbi` expects.

- **`capture_old` behavior is well-documented in the README but the POD is thin.** The inline POD in `CDC.pm` is a synopsis + a compact reference. Someone reading `perldoc DBIx::DataModel::Plugin::CDC` gets very little compared to the README. The POD for `Table.pm` and `Event.pm` is even sparser — almost nothing actionable.

- **Performance numbers have no date or version context.** The README says "Oracle Free container, N=100" but doesn't say which hardware or Oracle version. The numbers will drift. Minor, but worth a footnote.

### 1.2 Changes file — Good

Clear, concise, two versions. Follows standard CPAN conventions. Nothing to complain about.

### 1.3 Example README — Too thin

`examples/oracle-cdc-poc/README.md` says "run `./setup.sh`" and "requires podman". That's fine for getting it running, but there's no explanation of what the test does, how the schema maps to the plugin, or how to read the test output. A newcomer runs `setup.sh`, sees 51 tests pass, and has no idea what was validated.

---

## 2. BUILD & RUN EXPERIENCE

### 2.1 Unit tests

- `.gitignore` ignores `Makefile` and `MYMETA.*`, but both are committed and tracked. `git status` shows them as clean because they already exist. This is contradictory — either track them or don't. For a CPAN dist it's conventional to NOT track them (they're generated). Having `Makefile` (32KB auto-generated) in the repo is clutter.

- `blib/` is in `.gitignore` but there's a `blib/` directory with full copies of all `.pm` files in the repo. It looks like `make` was run and the output was committed. This is confusing — I see two copies of every module and have to figure out which one is authoritative.

- No `Makefile` target or script to "install deps + run tests" in one command. A `make devtest` or a simple shell script wrapping `cpanm --local-lib=... && prove ...` would help.

### 2.2 Integration tests

The `setup.sh` is solid — clean podman workflow, health checks, timeouts, colored output. It worked first try on my mental walk-through. The only friction: it assumes `podman` and `podman-compose`, not `docker`/`docker-compose`. The README mentions this but a fallback or detection would be friendlier.

### 2.3 Dockerfile

Good: multi-step, pinned versions, `USER nobody`, no secrets. Minor: downloads Oracle Instant Client from `download.oracle.com` at build time — if that URL changes or goes down, the build breaks. Consider noting this risk or caching the zip.

---

## 3. ARCHITECTURE

### 3.1 Module structure — Clear and minimal

Three modules, clear responsibilities. This is the project's biggest strength. I can hold the entire architecture in my head:

- `CDC.pm` = registry + listener management + query helpers
- `Table.pm` = hooks into DBIx::DataModel's DML methods
- `Event.pm` = event envelope factory

No abstract base classes, no plugins-of-plugins, no over-engineering. For a mid-level developer, this is approachable.

### 3.2 Concerns I'd raise

- **`CDC.pm` does too many things.** It's the registry, the listener dispatcher, the DBI logger, the stderr logger, AND the query helper layer (`events_for`, `event_pairs`, `clear_events`, etc.). The query helpers feel like they belong in a separate utility or could be left to the user. If I need to understand listener dispatch, I have to scroll past 100 lines of `events_for`/`count_events`/`event_pairs` that are unrelated. Not a blocker, but the file is 320 lines of code and growing.

- **Global mutable state via `%REGISTRY`.** The registry is a package-level hash keyed by schema class name. Calling `setup()` a second time silently overwrites everything (listeners, config). This is documented nowhere and bit me mentally when reading the tests — every subtest calls `setup()` again to reset state. In production, if someone calls `setup()` twice by accident, all listeners are gone. A warning or a `force => 1` option would be safer.

- **`_cdc_snapshot` takes `($class_or_self, $obj)` but is called as `_cdc_snapshot(undef, $rec)`.** The first argument is always ignored or `undef` in practice. This is a leftover from some earlier design. It works, but it's confusing — why does a snapshot function need a class/self?

- **No way to unregister a listener.** Once registered with `on()`, a listener lives forever in the registry. If I'm writing tests or doing hot-reload, I have to call `setup()` again which nukes everything. A `remove_listener` or returning a handle from `on()` would help.

---

## 4. CODE READABILITY

### 4.1 Overall impression — Good after the recent cleanup

The code reads well. Named variables, explicit loops, consistent style. The recent refactoring (replacing postfix-for, map/grep chains, bare-block closures) improved things significantly.

### 4.2 Remaining friction points

- **`_cdc_snapshot(undef, $obj)` call pattern.** Appears 6 times in `Table.pm`. Every time I read `_cdc_snapshot(undef, $rec)`, I stop and wonder what that `undef` is for. It's the `$class_or_self` parameter that's never used. Either make it a plain function `_cdc_snapshot($obj)` or document why it needs the first arg.

- **`uc()` is applied inconsistently across the codebase.** Table names are uppercased in `_cdc_table_name`. Column names are uppercased in `_cdc_snapshot`, `_cdc_pk_from`, `_cdc_event`, `_cdc_class_update`. But the uppercasing happens at different layers — sometimes in the helper, sometimes at the call site, sometimes in both. I had to trace through several functions to understand which layer is responsible for case normalization. A clear convention ("all CDC data uses uppercase keys; normalization happens in X") documented as a comment would help.

- **`_cdc_ensure_atomic` wantarray propagation.** Even with the comment, the `wantarray` capture + ternary dispatch across a `try` block is the hardest piece of code to follow in the project. I understand *why* it's needed, but if I had to modify this function, I'd be nervous about breaking context propagation. This is inherent Perl complexity though — not much to do about it.

- **`_is_named_args` is defined in `Table.pm` but feels like it could mislead.** The name suggests it checks for "named arguments" generically, but it actually checks for "first arg starts with `-`", which is specific to DBIx::DataModel's convention. A comment on the function saying "DBIx::DataModel class-method calls use `-set`, `-where` etc." would anchor this.

### 4.3 Test readability

The unit tests (`t/`) are clean and easy to follow. Each subtest has a clear name and a focused scope.

The integration test (`01_cdc_end_to_end.t`) is a 350+ line monolith. It's well-sectioned with comments, but it's one file covering 9 different aspects. If a test fails, the file/line number tells me where, but finding the *context* requires scrolling. Splitting into multiple files (like the unit tests) would help, though I understand the overhead of Oracle setup makes this impractical.

---

## 5. INCONSISTENCIES & SMALL BUGS

- **`cdc_events` table schema mismatch.** The Oracle DDL has `event_id NUMBER GENERATED ALWAYS AS IDENTITY`, `event_time`, `session_user`, `transaction_id`. But `log_to_dbi()` only inserts `(table_name, operation, old_data, new_data)`. The DB-generated columns fill themselves in, but the Perl-side `event_id` (from `Event.pm`) is never written to the DB. So the DB `event_id` is a sequence number, while the Perl `event_id` is a hex timestamp string. Same name, completely different values. This WILL confuse someone.

- **`Makefile` and `blib/` are tracked in git but also in `.gitignore`.** Clean this up.

- **`MANIFEST` includes `MANIFEST.SKIP` but `MANIFEST.SKIP` excludes `^Makefile$`.** This is standard CPAN practice and correct, but having Makefile both tracked in git and excluded from the dist is still confusing for a newcomer.

- **`Params::Validate` appears in the example cpanfile but isn't `use`d anywhere in the example code.** Vestigial dependency — remove it or use it.

- **`docker-compose.yml` has no `version:` field** — this is actually correct (Compose V2 doesn't need it), but the example README doesn't mention Compose V2 requirement. Minor.

---

## 6. SUMMARY — PRIORITIZED IMPROVEMENTS

| Priority | Item | Effort |
|----------|------|--------|
| **High** | Add a "Development" section to README (deps install, run tests, PERL5LIB) | 30 min |
| **High** | Remove `blib/` and `Makefile` from git tracking (they're generated) | 5 min |
| **High** | Add a generic (non-Oracle) `CREATE TABLE cdc_events` DDL to README showing the 4 columns `log_to_dbi` actually uses | 15 min |
| **High** | Clarify that DB `event_id` and Perl `event_id` are different things, or rename one | 30 min |
| **Medium** | Fix `_cdc_snapshot` signature — drop the unused `$class_or_self` first arg | 15 min |
| **Medium** | Document the "everything is uppercased" convention in one place | 10 min |
| **Medium** | Warn or croak if `setup()` is called twice for the same schema without explicit intent | 15 min |
| **Medium** | Expand POD in all three `.pm` files (especially `Table.pm` — it has 3 lines of POD for 360 lines of code) | 1 hr |
| **Medium** | Remove `Params::Validate` from example cpanfile if unused | 2 min |
| **Low** | Add a `make devtest` target or a `dev.sh` script | 15 min |
| **Low** | Consider splitting `CDC.pm` query helpers into a separate module | 30 min |
| **Low** | Add `remove_listener` or listener handles to `on()` | 30 min |
| **Low** | Docker/podman detection in `setup.sh` | 15 min |

---

## 7. VERDICT

This is a well-designed, well-scoped project. The architecture is clean, the code is readable (especially after the recent refactoring), and the test coverage is solid. The main friction is the onboarding experience — a new developer hits walls around "how do I install deps and run tests" and "what DB table do I need" before they can be productive. The code itself is not the problem; the gap between the README (which is consumer-focused) and the actual development workflow is.

I'd be comfortable maintaining this codebase.
