# Takeover Report — DBIx::DataModel::Plugin::CDC

*Perspective: mid-level Perl developer, first day on the project.*
*Date: 2026-04-08*
*Status: All actionable items remediated (see section 6).*

---

## 1. DOCUMENTATION

### 1.1 README.md — Very strong ~~, some gaps~~

The README is thorough and well-structured. Architecture diagram, event
envelope reference, listener API, performance numbers, known limitations
— it's all there. As a new developer, I could understand *what the project
does* and *how to use it* within 15 minutes.

~~Pain points~~ (all fixed):

- ~~No "Getting Started for Contributors" section.~~ **Fixed:** "Development" section added with `dev.sh` and manual instructions.
- ~~The `cdc_events` table DDL is only in the Oracle example.~~ **Fixed:** Generic DDL for SQLite, PostgreSQL, and Oracle added to README under `log_to_dbi`.
- ~~POD is thin.~~ **Fixed:** POD expanded in all three `.pm` files — Table.pm (DML interception, atomicity, naming convention), Event.pm (build() params, ID format), CDC.pm (query helpers).
- **Performance numbers have no date or version context.** The README says "Oracle Free container, N=100" but doesn't say which hardware or Oracle version. Minor, not addressed.

### 1.2 Changes file — Good

Clear, concise, two versions. Follows standard CPAN conventions.

### 1.3 Example README — ~~Too thin~~ Updated

~~`examples/oracle-cdc-poc/README.md` says "run `./setup.sh`" and "requires podman" with no further explanation.~~ **Fixed:** Example README now explains what the shared suite covers vs Oracle-specific tests, in a table.

---

## 2. BUILD & RUN EXPERIENCE

### 2.1 Unit tests

- ~~`.gitignore` ignores `Makefile` and `MYMETA.*`, but both are committed and tracked.~~ **Not an issue:** Verified they are NOT tracked in git.
- ~~`blib/` is in `.gitignore` but committed.~~ **Not an issue:** Verified `blib/` does not exist in the repo.
- ~~No script to "install deps + run tests".~~ **Fixed:** `dev.sh` with `setup`, `test`, `clean` subcommands.

### 2.2 Integration tests

~~`setup.sh` assumes podman only.~~ **Fixed:** Now auto-detects podman or docker, supports `podman-compose`, `docker-compose`, and `docker compose` plugin.

### 2.3 Dockerfile

Good: pinned versions, `USER nobody`, no secrets. Minor: downloads Oracle Instant Client at build time — if the URL changes, the build breaks. Not addressed (low risk).

---

## 3. ARCHITECTURE

### 3.1 Module structure — Clear and minimal

Three modules, clear responsibilities. This is the project's biggest strength.

- `CDC.pm` = registry + listener management + query helpers
- `Table.pm` = hooks into DBIx::DataModel's DML methods
- `Event.pm` = event envelope factory

### 3.2 Concerns raised

- **`CDC.pm` does too many things.** Registry, dispatcher, DBI/stderr loggers, and query helpers in one file. Not a blocker at 320 lines. **Decision:** Not splitting — the file is manageable and splitting adds a module for no user benefit.
- ~~**Global mutable state via `%REGISTRY` — silent overwrite on double `setup()`.**~~ **Fixed:** `setup()` now warns via `carp` when called again with existing listeners. Pass `force => 1` to suppress.
- ~~**`_cdc_snapshot(undef, $obj)` — unused first arg.**~~ **Fixed:** Signature simplified to `_cdc_snapshot($obj)`, all 6 call sites updated.
- **No way to unregister a listener.** **Decision:** YAGNI — `setup(force => 1)` resets everything, which covers the test and hot-reload use cases.

---

## 4. CODE READABILITY

### 4.1 Overall impression — Good

The code reads well after the readability refactoring (15 Perl idiom simplifications applied).

### 4.2 ~~Remaining friction points~~ Addressed

- ~~`_cdc_snapshot(undef, $obj)` call pattern.~~ **Fixed:** Signature now `_cdc_snapshot($obj)`.
- ~~`uc()` applied inconsistently.~~ **Fixed:** Convention block documented in `Table.pm` listing all four normalization points.
- **`_cdc_ensure_atomic` wantarray propagation.** Inherent Perl complexity — comment added explaining why `wantarray` is captured outside the `try` block. Not further simplifiable.
- ~~`_is_named_args` could mislead.~~ **Fixed:** Comment on the function explains the DBIx::DataModel `-set`/`-where` convention.

### 4.3 Test readability

Unit tests are clean. The integration test is a single file but well-sectioned. The shared test suite (`CDCTestSuite.pm`) now provides 30 reusable subtests used by both SQLite and Oracle backends.

---

## 5. INCONSISTENCIES & SMALL BUGS

- ~~**`cdc_events` table schema mismatch / `event_id` naming confusion.**~~ **Fixed:** Perl-side field renamed to `cdc_event_id`. README documents the distinction clearly.
- ~~**`Makefile` and `blib/` tracked in git.**~~ **Not an issue:** Verified they are not tracked.
- ~~**`Params::Validate` in example cpanfile unused.**~~ **Fixed:** Removed.
- **`docker-compose.yml` has no `version:` field.** Correct for Compose V2. Not an issue.

---

## 6. SUMMARY — REMEDIATION STATUS

| Priority | Item | Status |
|----------|------|--------|
| **High** | Add "Development" section to README + `dev.sh` | **Done** |
| **High** | Remove `blib/` and `Makefile` from git tracking | **Not an issue** (not tracked) |
| **High** | Add generic `CREATE TABLE cdc_events` DDL to README | **Done** |
| **High** | Clarify DB `event_id` vs Perl `event_id` | **Done** (renamed to `cdc_event_id`) |
| **Medium** | Fix `_cdc_snapshot` signature | **Done** |
| **Medium** | Document uppercase convention | **Done** |
| **Medium** | Warn on double `setup()` | **Done** (`carp` + `force => 1`) |
| **Medium** | Expand POD in all `.pm` files | **Done** |
| **Medium** | Remove `Params::Validate` from example cpanfile | **Done** |
| **Low** | Add `dev.sh` script | **Done** |
| **Low** | Split CDC.pm query helpers | **Won't do** (not worth it) |
| **Low** | Add `remove_listener` to `on()` | **Won't do** (YAGNI) |
| **Low** | Docker/podman detection in `setup.sh` | **Done** |

**Open items:** Performance numbers lack hardware context (minor, cosmetic).

---

## 7. VERDICT

This is a well-designed, well-scoped project. The architecture is clean, the code is readable, and the test coverage is solid (57 tests across unit and e2e layers, with a shared test suite that runs on both SQLite and Oracle). The onboarding experience is now smooth — `./dev.sh test` gets a new developer running in under a minute.

I'd be comfortable maintaining this codebase.
