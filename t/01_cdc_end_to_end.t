#!/usr/bin/env perl
# =============================================================
# t/01_cdc_end_to_end.t
#
# Comprehensive end-to-end tests for ORM-hook-based CDC.
# Covers: CRUD, transactions, atomicity, edge cases.
#
# Prerequisites:
#   - Oracle Free container running and healthy
#   - SQL init scripts executed (tables in place)
#   - Perl deps installed:  cpanm --installdeps .
#
# Run:
#   prove -lv t/01_cdc_end_to_end.t
# =============================================================

use strict;
use warnings;
use utf8;
use open ':std', ':encoding(UTF-8)';

use Test::More;
use Test::Exception;
use DBI;
use Try::Tiny;

use lib 'lib';
use App::Schema;
use CDC::Manager;

# ---------------------------------------------------------------
# 0.  Connect and install CDC hooks
# ---------------------------------------------------------------
my $dsn  = $ENV{ORACLE_DSN}
         // 'dbi:Oracle:host=localhost;port=1521;service_name=FREEPDB1';
my $user = $ENV{ORACLE_USER} // 'appuser';
my $pass = $ENV{ORACLE_PASS} // 'apppass';

my $dbh = DBI->connect($dsn, $user, $pass, {
    RaiseError       => 1,
    PrintError       => 0,
    AutoCommit       => 1,
    FetchHashKeyName => 'NAME_lc',
    ora_charset      => 'UTF8',
    LongReadLen      => 1_000_000,
    LongTruncOk      => 0,
}) or BAIL_OUT("Cannot connect to Oracle: $DBI::errstr");

App::Schema->dbh($dbh);
my $cdc = App::Schema->install_cdc($dbh);

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
sub clean_tables {
    local $dbh->{AutoCommit} = 1;
    $dbh->do('DELETE FROM employees');
    $dbh->do('DELETE FROM departments');
    $cdc->clear_events();
    return;
}

sub _insert_dept {
    my (%args) = @_;
    App::Schema->table('Department')->insert({
        name     => $args{name}     // 'TestDept',
        location => $args{location} // 'Geneva',
    });
    my $ev = $cdc->latest_event(table => 'departments', operation => 'INSERT')
        or die 'No INSERT event captured for department';
    return $cdc->parse_row_image($ev->{new_data})->{ID};
}

sub _fetch_dept {
    my ($where) = @_;
    my ($row) = @{ App::Schema->table('Department')->select(-where => $where) };
    return $row;
}

sub _fetch_emp {
    my ($where) = @_;
    my ($row) = @{ App::Schema->table('Employee')->select(-where => $where) };
    return $row;
}

# ===============================================================
# SECTION 1: INFRASTRUCTURE
# ===============================================================

subtest 'Infrastructure – connectivity and table existence' => sub {
    plan tests => 4;

    ok($dbh->ping, 'Database connection alive');

    for my $tbl (qw/DEPARTMENTS EMPLOYEES CDC_EVENTS/) {
        my ($n) = $dbh->selectrow_array(
            q{SELECT COUNT(*) FROM user_tables WHERE table_name = ?},
            undef, $tbl
        );
        ok($n, "Table $tbl exists");
    }
};

# ===============================================================
# SECTION 2: BASIC CRUD CAPTURE
# ===============================================================

subtest 'INSERT capture – Department via ORM' => sub {
    plan tests => 6;
    clean_tables();

    App::Schema->table('Department')->insert({
        name     => 'Engineering',
        location => 'Geneva',
    });

    my $events = $cdc->events_for(table => 'departments', operation => 'INSERT');
    is(scalar @$events, 1,        'Exactly one INSERT event');
    my $ev = $events->[0];
    is($ev->{operation},  'INSERT', 'operation field is INSERT');
    ok(!defined $ev->{old_data},   'old_data is NULL for INSERT');
    ok(defined  $ev->{new_data},   'new_data is populated');

    my $new = $cdc->parse_row_image($ev->{new_data});
    is($new->{NAME},     'Engineering', 'new_data NAME correct');
    is($new->{LOCATION}, 'Geneva',      'new_data LOCATION correct');
};

subtest 'INSERT capture – Employee via ORM with FK' => sub {
    plan tests => 4;
    clean_tables();

    my $dept_id = _insert_dept(name => 'HR', location => 'Zurich');
    $cdc->clear_events();

    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name    => 'Alice',
        last_name     => 'Dupont',
        email         => 'alice@example.com',
        salary        => 90_000,
    });

    my $ev = $cdc->latest_event(table => 'employees', operation => 'INSERT');
    ok($ev, 'INSERT event captured');

    my $new = $cdc->parse_row_image($ev->{new_data});
    is($new->{FIRST_NAME},    'Alice',             'first_name captured');
    is($new->{EMAIL},         'alice@example.com', 'email captured');
    is($new->{DEPARTMENT_ID}, $dept_id,            'FK captured');
};

subtest 'UPDATE capture – via ORM instance method' => sub {
    plan tests => 4;
    clean_tables();

    my $dept_id = _insert_dept(name => 'Legal', location => 'Lausanne');
    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name    => 'Bob',
        last_name     => 'Martin',
        email         => 'bob@example.com',
        salary        => 75_000,
    });
    $cdc->clear_events();

    my $emp = _fetch_emp({ email => 'bob@example.com' });
    $emp->update({ salary => 80_000 });

    my $ev = $cdc->latest_event(table => 'employees', operation => 'UPDATE');
    ok($ev, 'UPDATE event captured');
    is($ev->{operation}, 'UPDATE', 'operation is UPDATE');

    my $old = $cdc->parse_row_image($ev->{old_data});
    my $new = $cdc->parse_row_image($ev->{new_data});

    like($old->{SALARY}, qr/^75000/, 'old salary is 75000');
    like($new->{SALARY}, qr/^80000/, 'new salary is 80000');
};

subtest 'DELETE capture – via ORM' => sub {
    plan tests => 4;
    clean_tables();

    App::Schema->table('Department')->insert({
        name => 'Marketing', location => 'Geneva',
    });
    my $dept = _fetch_dept({ name => 'Marketing' });
    $cdc->clear_events();

    $dept->delete();

    my $ev = $cdc->latest_event(table => 'departments', operation => 'DELETE');
    ok($ev, 'DELETE event captured');
    is($ev->{operation}, 'DELETE', 'operation is DELETE');
    ok(!defined $ev->{new_data},   'new_data is NULL for DELETE');

    my $old = $cdc->parse_row_image($ev->{old_data});
    is($old->{NAME}, 'Marketing', 'old_data NAME correct');
};

# ===============================================================
# SECTION 3: TRANSACTION SAFETY
# ===============================================================

subtest 'ROLLBACK – explicit rollback discards DML and CDC events' => sub {
    plan tests => 2;
    clean_tables();

    {
        local $dbh->{AutoCommit} = 0;
        try {
            App::Schema->table('Department')->insert({
                name => 'Rollback', location => 'X',
            });
            $dbh->rollback();
        } catch {
            $dbh->rollback();
        };
    }

    is($cdc->count_events(table => 'departments'), 0,
        'Zero CDC events after ROLLBACK');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM departments');
    is($n, 0, 'Zero rows in departments after ROLLBACK');
};

subtest 'COMMIT – multi-statement transaction commits atomically' => sub {
    plan tests => 3;
    clean_tables();

    {
        local $dbh->{AutoCommit} = 0;
        App::Schema->table('Department')->insert({
            name => 'Ops1', location => 'Zurich',
        });
        App::Schema->table('Department')->insert({
            name => 'Ops2', location => 'Zurich',
        });
        $dbh->commit();
    }

    is($cdc->count_events(table => 'departments', operation => 'INSERT'),
        2, 'Two INSERT events after COMMIT');
    is($cdc->count_events(table => 'departments', operation => 'UPDATE'),
        0, 'No UPDATE events');
    is($cdc->count_events(table => 'departments', operation => 'DELETE'),
        0, 'No DELETE events');
};

subtest 'AutoCommit ON – DML + CDC event are atomic (mini-transaction)' => sub {
    plan tests => 2;
    clean_tables();

    # With AutoCommit ON (the default), the hook wraps DML + CDC
    # write in a mini-transaction.  Both must succeed or neither.
    ok($dbh->{AutoCommit}, 'AutoCommit is ON for this test');

    App::Schema->table('Department')->insert({
        name => 'Atomic', location => 'Bern',
    });

    # Verify both the row and the CDC event exist
    my ($n) = $dbh->selectrow_array(
        "SELECT COUNT(*) FROM departments WHERE name = 'Atomic'"
    );
    is($cdc->count_events(table => 'departments', operation => 'INSERT'),
        $n, 'CDC event count matches row count (atomicity)');
};

subtest 'Transaction – INSERT + UPDATE + DELETE in single txn' => sub {
    plan tests => 4;
    clean_tables();

    {
        local $dbh->{AutoCommit} = 0;

        App::Schema->table('Department')->insert({
            name => 'Lifecycle', location => 'Bern',
        });

        my $dept = _fetch_dept({ name => 'Lifecycle' });
        $dept->update({ location => 'Basel' });
        $dept->delete();

        $dbh->commit();
    }

    is($cdc->count_events(table => 'departments', operation => 'INSERT'),
        1, 'INSERT event captured in txn');
    is($cdc->count_events(table => 'departments', operation => 'UPDATE'),
        1, 'UPDATE event captured in txn');
    is($cdc->count_events(table => 'departments', operation => 'DELETE'),
        1, 'DELETE event captured in txn');

    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM departments');
    is($n, 0, 'Row deleted after full lifecycle');
};

subtest 'Transaction – partial rollback after second insert fails' => sub {
    plan tests => 2;
    clean_tables();

    my $caught = 0;
    {
        local $dbh->{AutoCommit} = 0;
        try {
            App::Schema->table('Department')->insert({
                name => 'First', location => 'OK',
            });
            # Insert with name longer than VARCHAR2(100) to cause error
            App::Schema->table('Department')->insert({
                name => 'A' x 200, location => 'Overflow',
            });
            $dbh->commit();
        } catch {
            $caught = 1;
            $dbh->rollback();
        };
    }

    ok($caught, 'Exception caught on oversized insert');
    is($cdc->count_events(table => 'departments'), 0,
        'Zero CDC events — entire transaction rolled back');
};

subtest 'Transaction – interleaved tables in single txn' => sub {
    plan tests => 2;
    clean_tables();

    {
        local $dbh->{AutoCommit} = 0;

        App::Schema->table('Department')->insert({
            name => 'Multi', location => 'Zurich',
        });
        my $dept = _fetch_dept({ name => 'Multi' });

        App::Schema->table('Employee')->insert({
            department_id => $dept->{id},
            first_name    => 'Zara',
            last_name     => 'Koenig',
            email         => 'zara@example.com',
            salary        => 95_000,
        });

        $dbh->commit();
    }

    is($cdc->count_events(table => 'departments', operation => 'INSERT'),
        1, 'Department INSERT captured');
    is($cdc->count_events(table => 'employees', operation => 'INSERT'),
        1, 'Employee INSERT captured in same txn');
};

subtest 'Constraint violation – no CDC event on failed INSERT' => sub {
    plan tests => 2;
    clean_tables();

    App::Schema->table('Employee')->insert({
        first_name => 'Eve',
        last_name  => 'X',
        email      => 'eve@example.com',
        salary     => 50_000,
    });
    $cdc->clear_events();

    my $failed = 0;
    try {
        App::Schema->table('Employee')->insert({
            first_name => 'Eve2',
            last_name  => 'Y',
            email      => 'eve@example.com',  # duplicate unique key
            salary     => 60_000,
        });
    } catch {
        $failed = 1;
    };

    ok($failed, 'Duplicate INSERT correctly rejected');
    is($cdc->count_events(table => 'employees'), 0,
        'No CDC event for failed INSERT');
};

subtest 'Constraint violation in txn – rollback cleans all' => sub {
    plan tests => 3;
    clean_tables();

    my $caught = 0;
    {
        local $dbh->{AutoCommit} = 0;
        try {
            App::Schema->table('Employee')->insert({
                first_name => 'Good',
                last_name  => 'Row',
                email      => 'good@example.com',
                salary     => 50_000,
            });
            App::Schema->table('Employee')->insert({
                first_name => 'Good',
                last_name  => 'Row',
                email      => 'good@example.com',  # duplicate
                salary     => 60_000,
            });
            $dbh->commit();
        } catch {
            $caught = 1;
            $dbh->rollback();
        };
    }

    ok($caught, 'Duplicate caught in transaction');
    is($cdc->count_events(table => 'employees'), 0,
        'Zero CDC events after txn rollback');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM employees');
    is($n, 0, 'Zero rows after txn rollback');
};

# ===============================================================
# SECTION 3b: CLASS-METHOD UPDATE / DELETE
# ===============================================================

subtest 'Class-method UPDATE – captures per-row old/new' => sub {
    plan tests => 5;
    clean_tables();

    my $dept_id = _insert_dept(name => 'ClassUpd', location => 'Bern');
    for my $i (1..3) {
        App::Schema->table('Employee')->insert({
            department_id => $dept_id,
            first_name    => "CM$i",
            last_name     => "Last$i",
            email         => "cm${i}\@example.com",
            salary        => 50_000,
        });
    }
    $cdc->clear_events();

    App::Schema->table('Employee')->update(
        -set   => { salary => 55_000 },
        -where => { department_id => $dept_id },
    );

    is($cdc->count_events(table => 'employees', operation => 'UPDATE'),
        3, 'One UPDATE event per affected row');

    my $pairs = $cdc->event_pairs(table => 'employees');
    is(scalar @$pairs, 3, 'Three pairs returned');

    my ($old, $new) = @{ $pairs->[0] };
    like($old->{SALARY}, qr/^50000/, 'old salary captured');
    like($new->{SALARY}, qr/^55000/, 'new salary captured');
    ok(defined $old->{FIRST_NAME}, 'Full row snapshot in old_data');
};

subtest 'Class-method DELETE – captures per-row old_data' => sub {
    plan tests => 3;
    clean_tables();

    for my $i (1..2) {
        App::Schema->table('Department')->insert({
            name => "Del$i", location => 'X',
        });
    }
    $cdc->clear_events();

    App::Schema->table('Department')->delete(
        -where => { location => 'X' },
    );

    is($cdc->count_events(table => 'departments', operation => 'DELETE'),
        2, 'One DELETE event per affected row');

    my $events = $cdc->events_for(table => 'departments', operation => 'DELETE');
    my $old1 = $cdc->parse_row_image($events->[0]{old_data});
    my $old2 = $cdc->parse_row_image($events->[1]{old_data});

    ok(defined $old1->{NAME}, 'First deleted row has NAME in old_data');
    ok(!defined $events->[0]{new_data}, 'new_data is NULL for DELETE');
};

subtest 'Class-method UPDATE with no matching rows – zero events' => sub {
    plan tests => 1;
    clean_tables();

    App::Schema->table('Department')->insert({
        name => 'NoMatch', location => 'X',
    });
    $cdc->clear_events();

    App::Schema->table('Department')->update(
        -set   => { location => 'Y' },
        -where => { name => 'NonExistent' },
    );

    is($cdc->count_events(table => 'departments', operation => 'UPDATE'),
        0, 'Zero events when no rows match');
};

# ===============================================================
# SECTION 4: DATA INTEGRITY & EDGE CASES
# ===============================================================

subtest 'NULL column values captured correctly' => sub {
    plan tests => 3;
    clean_tables();

    App::Schema->table('Department')->insert({
        name     => 'Ghost',
        location => undef,
    });

    my $ev  = $cdc->latest_event(table => 'departments', operation => 'INSERT');
    my $new = $cdc->parse_row_image($ev->{new_data});

    ok($ev, 'INSERT with NULL column captured');
    is($new->{NAME},     'Ghost', 'NAME captured correctly');
    is($new->{LOCATION}, undef,   'NULL LOCATION parsed as undef');
};

subtest 'UPDATE preserves unchanged columns in new_data' => sub {
    plan tests => 3;
    clean_tables();

    App::Schema->table('Department')->insert({
        name => 'Stable', location => 'Bern',
    });
    my $dept = _fetch_dept({ name => 'Stable' });
    $cdc->clear_events();

    $dept->update({ location => 'Basel' });

    my $ev  = $cdc->latest_event(table => 'departments', operation => 'UPDATE');
    my $old = $cdc->parse_row_image($ev->{old_data});
    my $new = $cdc->parse_row_image($ev->{new_data});

    is($old->{NAME}, $new->{NAME}, 'Unchanged NAME identical in old/new');
    is($old->{ID},   $new->{ID},   'PK unchanged between old/new');
    isnt($old->{LOCATION}, $new->{LOCATION}, 'Changed LOCATION differs');
};

subtest 'Bulk INSERT via ORM – one event per row' => sub {
    plan tests => 1;
    clean_tables();

    my $dept_id = _insert_dept(name => 'Bulk', location => 'Bern');
    $cdc->clear_events();

    for my $i (1 .. 5) {
        App::Schema->table('Employee')->insert({
            department_id => $dept_id,
            first_name    => "User$i",
            last_name     => "Last$i",
            email         => "user${i}\@example.com",
            salary        => 50_000 + $i * 1_000,
        });
    }

    is($cdc->count_events(table => 'employees', operation => 'INSERT'),
        5, '5 INSERT events captured');
};

subtest 'Bulk UPDATE via ORM – one event per row' => sub {
    plan tests => 1;
    clean_tables();

    my $dept_id = _insert_dept(name => 'BulkU', location => 'Bern');
    for my $i (1..3) {
        App::Schema->table('Employee')->insert({
            department_id => $dept_id,
            first_name    => "Upd$i",
            last_name     => "Last$i",
            email         => "upd${i}\@example.com",
            salary        => 60_000,
        });
    }
    $cdc->clear_events();

    for my $emp (@{ App::Schema->table('Employee')
            ->select(-where => { department_id => $dept_id }) }) {
        $emp->update({ salary => 65_000 });
    }

    is($cdc->count_events(table => 'employees', operation => 'UPDATE'),
        3, '3 UPDATE events for 3 rows');
};

subtest 'Cross-table FK – parent and child tracked' => sub {
    plan tests => 4;
    clean_tables();

    App::Schema->table('Department')->insert({
        name => 'CrossRef', location => 'Zug',
    });
    my $dept_ev = $cdc->latest_event(table => 'departments', operation => 'INSERT');
    ok($dept_ev, 'Department INSERT tracked');

    my $dept_id = $cdc->parse_row_image($dept_ev->{new_data})->{ID};

    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name    => 'Frank',
        last_name     => 'Z',
        email         => 'frank@example.com',
        salary        => 80_000,
    });
    my $emp_ev = $cdc->latest_event(table => 'employees', operation => 'INSERT');
    ok($emp_ev, 'Employee INSERT tracked');

    my $emp_row = $cdc->parse_row_image($emp_ev->{new_data});
    is($emp_row->{FIRST_NAME},    'Frank',    'Employee first_name captured');
    is($emp_row->{DEPARTMENT_ID}, $dept_id,   'FK to department captured');
};

subtest 'Special characters – accents, apostrophe, dash' => sub {
    plan tests => 2;
    clean_tables();

    my $name = "R&D / O'Brien \x{2013} caf\x{e9}";
    App::Schema->table('Department')->insert({
        name     => $name,
        location => "Neuch\x{e2}tel",
    });

    my $ev  = $cdc->latest_event(table => 'departments', operation => 'INSERT');
    my $new = $cdc->parse_row_image($ev->{new_data});

    ok($ev, 'INSERT with special characters captured');
    is($new->{NAME}, $name, 'Special characters round-trip correctly');
};

subtest 'CDC event metadata – event_id, event_time, table_name' => sub {
    plan tests => 4;
    clean_tables();

    App::Schema->table('Department')->insert({ name => 'Meta', location => 'Bern' });

    my $ev = $cdc->latest_event(table => 'departments', operation => 'INSERT');
    ok($ev,                        'Event row present');
    ok($ev->{event_id} > 0,        'event_id is a positive integer');
    ok(defined $ev->{event_time},  'event_time is set');
    is($ev->{table_name}, 'DEPARTMENTS', 'table_name is stored in upper-case');
};

subtest 'Event ordering – events ordered by event_id ASC' => sub {
    plan tests => 2;
    clean_tables();

    for my $i (1..3) {
        App::Schema->table('Department')->insert({
            name => "Ordered$i", location => 'X',
        });
    }

    my $events = $cdc->events_for(table => 'departments');
    is(scalar @$events, 3, 'Three events captured');

    my $ordered = 1;
    for my $i (1 .. $#$events) {
        $ordered = 0 if $events->[$i]{event_id} <= $events->[$i-1]{event_id};
    }
    ok($ordered, 'Events are in ascending event_id order');
};

# ===============================================================
# SECTION 5: HELPER METHODS
# ===============================================================

subtest 'event_pairs() helper – UPDATE old/new as paired hashes' => sub {
    plan tests => 3;
    clean_tables();

    my $dept_id = _insert_dept(name => 'Pair', location => 'Bern');
    for my $i (1..2) {
        App::Schema->table('Employee')->insert({
            department_id => $dept_id,
            first_name    => "P$i",
            last_name     => "L$i",
            email         => "pair${i}\@example.com",
            salary        => 60_000,
        });
    }
    $cdc->clear_events();

    for my $emp (@{ App::Schema->table('Employee')
            ->select(-where => { department_id => $dept_id }) }) {
        $emp->update({ salary => 65_000 });
    }

    my $pairs = $cdc->event_pairs(table => 'employees');
    is(scalar @$pairs, 2, 'Two UPDATE pairs returned');

    my ($old0, $new0) = @{ $pairs->[0] };
    like($old0->{SALARY}, qr/^60000/, 'First old salary is 60000');
    like($new0->{SALARY}, qr/^65000/, 'First new salary is 65000');
};

subtest 'count_events filters by operation' => sub {
    plan tests => 4;
    clean_tables();

    App::Schema->table('Department')->insert({
        name => 'Count', location => 'Bern',
    });
    my $dept = _fetch_dept({ name => 'Count' });
    $dept->update({ location => 'Basel' });

    is($cdc->count_events(table => 'departments'), 2, 'Total events = 2');
    is($cdc->count_events(table => 'departments', operation => 'INSERT'),
        1, 'INSERT count = 1');
    is($cdc->count_events(table => 'departments', operation => 'UPDATE'),
        1, 'UPDATE count = 1');
    is($cdc->count_events(table => 'departments', operation => 'DELETE'),
        0, 'DELETE count = 0');
};

subtest 'clear_events_for clears only one table' => sub {
    plan tests => 2;
    clean_tables();

    my $dept_id = _insert_dept(name => 'Selective', location => 'X');
    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name    => 'Sel',
        last_name     => 'Test',
        email         => 'sel@example.com',
        salary        => 50_000,
    });

    $cdc->clear_events_for(table => 'departments');

    is($cdc->count_events(table => 'departments'), 0,
        'Department events cleared');
    is($cdc->count_events(table => 'employees'), 1,
        'Employee events preserved');
};

# ===============================================================
# SECTION 6: DESIGN TRADE-OFFS
# ===============================================================

subtest 'Raw DBI bypass – not captured (expected trade-off)' => sub {
    plan tests => 1;
    clean_tables();

    $dbh->do(
        q{INSERT INTO departments(name, location) VALUES(?, ?)},
        undef, 'RawDBI', 'Nowhere',
    );

    is($cdc->count_events(table => 'departments'), 0,
        'Raw DBI INSERT produces zero CDC events (by design)');
};

# ---------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------
clean_tables();
$dbh->disconnect();

done_testing();
