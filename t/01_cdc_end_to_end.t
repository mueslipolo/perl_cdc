#!/usr/bin/env perl
# =============================================================
# t/01_cdc_end_to_end.t
#
# End-to-end tests for ORM-hook-based CDC.
# CDC events are captured by wrapping DBIx::DataModel methods
# in Perl — no database triggers or stored procedures needed.
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

# ---------------------------------------------------------------
# 1.  INFRASTRUCTURE
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# 2.  INSERT via ORM
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# 3.  INSERT – Employee via ORM with FK
# ---------------------------------------------------------------
subtest 'INSERT capture – Employee via ORM' => sub {
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

# ---------------------------------------------------------------
# 4.  UPDATE via ORM (instance method)
# ---------------------------------------------------------------
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

    my ($emp) = @{ App::Schema->table('Employee')
        ->select(-where => { email => 'bob@example.com' }) };
    $emp->update({ salary => 80_000 });

    my $ev = $cdc->latest_event(table => 'employees', operation => 'UPDATE');
    ok($ev, 'UPDATE event captured');
    is($ev->{operation}, 'UPDATE', 'operation is UPDATE');

    my $old = $cdc->parse_row_image($ev->{old_data});
    my $new = $cdc->parse_row_image($ev->{new_data});

    like($old->{SALARY}, qr/^75000/, 'old salary is 75000');
    like($new->{SALARY}, qr/^80000/, 'new salary is 80000');
};

# ---------------------------------------------------------------
# 5.  DELETE via ORM
# ---------------------------------------------------------------
subtest 'DELETE capture – via ORM' => sub {
    plan tests => 4;
    clean_tables();

    App::Schema->table('Department')->insert({
        name => 'Marketing', location => 'Geneva',
    });
    my ($dept) = @{ App::Schema->table('Department')
        ->select(-where => { name => 'Marketing' }) };
    $cdc->clear_events();

    $dept->delete();

    my $ev = $cdc->latest_event(table => 'departments', operation => 'DELETE');
    ok($ev, 'DELETE event captured');
    is($ev->{operation}, 'DELETE', 'operation is DELETE');
    ok(!defined $ev->{new_data},   'new_data is NULL for DELETE');

    my $old = $cdc->parse_row_image($ev->{old_data});
    is($old->{NAME}, 'Marketing', 'old_data NAME correct');
};

# ---------------------------------------------------------------
# 6.  ROLLBACK – no ghost events
# ---------------------------------------------------------------
subtest 'ROLLBACK – no CDC events emitted' => sub {
    plan tests => 1;
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
        'Zero events after ROLLBACK');
};

# ---------------------------------------------------------------
# 7.  COMMIT – multi-statement transaction
# ---------------------------------------------------------------
subtest 'COMMIT – multi-statement transaction' => sub {
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

# ---------------------------------------------------------------
# 8.  NULL column values
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# 9.  Bulk INSERT via ORM – one event per row
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# 10. Cross-table FK – parent and child tracked
# ---------------------------------------------------------------
subtest 'Cross-table FK – INSERT on parent and child tracked' => sub {
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

# ---------------------------------------------------------------
# 11. Special characters and encoding
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# 12. CDC event metadata
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# 13. event_pairs() helper – UPDATE old/new as paired hashes
# ---------------------------------------------------------------
subtest 'CDC::Manager event_pairs() helper' => sub {
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

    for my $emp (
        @{ App::Schema->table('Employee')
            ->select(-where => { department_id => $dept_id }) }
    ) {
        $emp->update({ salary => 65_000 });
    }

    my $pairs = $cdc->event_pairs(table => 'employees');
    is(scalar @$pairs, 2, 'Two UPDATE pairs returned');

    my ($old0, $new0) = @{ $pairs->[0] };
    like($old0->{SALARY}, qr/^60000/, 'First old salary is 60000');
    like($new0->{SALARY}, qr/^65000/, 'First new salary is 65000');
};

# ---------------------------------------------------------------
# 14. Constraint violation – no CDC event on failed DML
# ---------------------------------------------------------------
subtest 'Constraint violation – no CDC event on failed ORM insert' => sub {
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
            email      => 'eve@example.com',  # duplicate
            salary     => 60_000,
        });
    } catch {
        $failed = 1;
    };

    ok($failed, 'Duplicate INSERT correctly rejected');
    is($cdc->count_events(table => 'employees'), 0,
        'No CDC event recorded for failed INSERT');
};

# ---------------------------------------------------------------
# 15. Raw DBI bypass – ORM hooks do not capture raw SQL
# ---------------------------------------------------------------
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
