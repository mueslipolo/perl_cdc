#!/usr/bin/env perl
# =============================================================
# t/01_cdc_end_to_end.t
#
# End-to-end tests for DBIx::DataModel::Plugin::CDC.
# Uses the table_parent extension mechanism with pluggable handlers.
# =============================================================

use strict;
use warnings;
use utf8;
use open ':std', ':encoding(UTF-8)';

use Test::More;
use Test::Exception;
use DBI;
use Try::Tiny;
use Cpanel::JSON::XS ();

use lib 'lib';
use App::Schema;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Handler::DBI;
use DBIx::DataModel::Plugin::CDC::Handler::Callback;
use DBIx::DataModel::Plugin::CDC::Handler::Log;
use DBIx::DataModel::Plugin::CDC::Handler::Multi;

my $JSON = Cpanel::JSON::XS->new->utf8->canonical->allow_nonref;

# ---------------------------------------------------------------
# 0.  Connect and configure CDC
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

# Collect events dispatched to callback handler
my @callback_events;

my $dbi_handler = DBIx::DataModel::Plugin::CDC::Handler::DBI->new(
    table_name => 'cdc_events',
);

my $cb_handler = DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
    on_event => sub { push @callback_events, $_[0] },
    phase    => 'post_commit',
    on_error => 'warn',
);

DBIx::DataModel::Plugin::CDC->setup('App::Schema',
    tables   => 'all',
    handlers => [$dbi_handler, $cb_handler],
);

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
my $CDC = 'DBIx::DataModel::Plugin::CDC';

sub clean_tables {
    local $dbh->{AutoCommit} = 1;
    $dbh->do('DELETE FROM employees');
    $dbh->do('DELETE FROM departments');
    $CDC->clear_events('App::Schema');
    @callback_events = ();
    return;
}

sub _insert_dept {
    my (%args) = @_;
    App::Schema->table('Department')->insert({
        name     => $args{name}     // 'TestDept',
        location => $args{location} // 'Geneva',
    });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT')
        or die 'No INSERT event captured for department';
    my $data = $JSON->decode($ev->{new_data});
    return $data->{ID};
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

my $JSON_DECODE = Cpanel::JSON::XS->new->canonical->allow_nonref;

sub _parse {
    my ($json_str) = @_;
    return undef unless defined $json_str;
    # Oracle returns Perl character strings (wide), not raw bytes
    return $JSON_DECODE->decode($json_str);
}

# ===============================================================
# SECTION 1: INFRASTRUCTURE
# ===============================================================

subtest 'Infrastructure – connectivity and tables' => sub {
    plan tests => 4;
    ok($dbh->ping, 'Database connection alive');
    for my $tbl (qw/DEPARTMENTS EMPLOYEES CDC_EVENTS/) {
        my ($n) = $dbh->selectrow_array(
            q{SELECT COUNT(*) FROM user_tables WHERE table_name = ?},
            undef, $tbl);
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
        name => 'Engineering', location => 'Geneva',
    });

    my $events = $CDC->events_for('App::Schema',
        table => 'departments', operation => 'INSERT');
    is(scalar @$events, 1,       'Exactly one INSERT event');
    my $ev = $events->[0];
    is($ev->{operation}, 'INSERT', 'operation is INSERT');
    ok(!defined $ev->{old_data},   'old_data is NULL');
    ok(defined  $ev->{new_data},   'new_data is populated');

    my $new = _parse($ev->{new_data});
    is($new->{NAME},     'Engineering', 'NAME correct');
    is($new->{LOCATION}, 'Geneva',      'LOCATION correct');
};

subtest 'INSERT capture – Employee via ORM with FK' => sub {
    plan tests => 4;
    clean_tables();
    my $dept_id = _insert_dept(name => 'HR', location => 'Zurich');
    $CDC->clear_events('App::Schema');

    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name    => 'Alice',
        last_name     => 'Dupont',
        email         => 'alice@example.com',
        salary        => 90_000,
    });

    my $ev  = $CDC->latest_event('App::Schema',
        table => 'employees', operation => 'INSERT');
    ok($ev, 'INSERT event captured');
    my $new = _parse($ev->{new_data});
    is($new->{FIRST_NAME},    'Alice',             'first_name captured');
    is($new->{EMAIL},         'alice@example.com', 'email captured');
    is($new->{DEPARTMENT_ID}, $dept_id,            'FK captured');
};

subtest 'UPDATE capture – instance method' => sub {
    plan tests => 3;
    clean_tables();
    my $dept_id = _insert_dept(name => 'Legal', location => 'Lausanne');
    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name => 'Bob', last_name => 'Martin',
        email => 'bob@example.com', salary => 75_000,
    });
    $CDC->clear_events('App::Schema');

    my $emp = _fetch_emp({ email => 'bob@example.com' });
    $emp->update({ salary => 80_000 });

    my $ev = $CDC->latest_event('App::Schema',
        table => 'employees', operation => 'UPDATE');
    ok($ev, 'UPDATE event captured');

    my $old = _parse($ev->{old_data});
    my $new = _parse($ev->{new_data});
    like($old->{SALARY}, qr/^75000/, 'old salary 75000');
    like($new->{SALARY}, qr/^80000/, 'new salary 80000');
};

subtest 'DELETE capture – instance method' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({
        name => 'Marketing', location => 'Geneva',
    });
    my $dept = _fetch_dept({ name => 'Marketing' });
    $CDC->clear_events('App::Schema');

    $dept->delete();

    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'DELETE');
    ok($ev, 'DELETE event captured');
    is($ev->{operation}, 'DELETE', 'operation is DELETE');
    ok(!defined $ev->{new_data}, 'new_data is NULL');
    my $old = _parse($ev->{old_data});
    is($old->{NAME}, 'Marketing', 'old NAME correct');
};

# ===============================================================
# SECTION 3: TRANSACTION SAFETY
# ===============================================================

subtest 'ROLLBACK – discards DML and CDC events' => sub {
    plan tests => 2;
    clean_tables();
    {
        local $dbh->{AutoCommit} = 0;
        try {
            App::Schema->table('Department')->insert({
                name => 'Rollback', location => 'X',
            });
            $dbh->rollback();
        } catch { $dbh->rollback() };
    }
    is($CDC->count_events('App::Schema', table => 'departments'), 0,
        'Zero CDC events after ROLLBACK');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM departments');
    is($n, 0, 'Zero rows after ROLLBACK');
};

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
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'INSERT'), 2, '2 INSERT events');
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'UPDATE'), 0, '0 UPDATE events');
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'DELETE'), 0, '0 DELETE events');
};

subtest 'AutoCommit ON – DML + CDC event are atomic' => sub {
    plan tests => 2;
    clean_tables();
    ok($dbh->{AutoCommit}, 'AutoCommit is ON');
    App::Schema->table('Department')->insert({
        name => 'Atomic', location => 'Bern',
    });
    my ($n) = $dbh->selectrow_array(
        "SELECT COUNT(*) FROM departments WHERE name = 'Atomic'");
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'INSERT'),
        $n, 'CDC event count matches row count');
};

subtest 'Transaction – INSERT + UPDATE + DELETE lifecycle' => sub {
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
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'INSERT'), 1, 'INSERT in txn');
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'UPDATE'), 1, 'UPDATE in txn');
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'DELETE'), 1, 'DELETE in txn');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM departments');
    is($n, 0, 'Row deleted after lifecycle');
};

subtest 'Partial rollback after second insert fails' => sub {
    plan tests => 2;
    clean_tables();
    my $caught = 0;
    {
        local $dbh->{AutoCommit} = 0;
        try {
            App::Schema->table('Department')->insert({
                name => 'First', location => 'OK',
            });
            App::Schema->table('Department')->insert({
                name => 'A' x 200, location => 'Overflow',
            });
            $dbh->commit();
        } catch { $caught = 1; $dbh->rollback() };
    }
    ok($caught, 'Exception caught');
    is($CDC->count_events('App::Schema', table => 'departments'), 0,
        'Zero events after rollback');
};

subtest 'Interleaved tables in single txn' => sub {
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
            first_name => 'Zara', last_name => 'Koenig',
            email => 'zara@example.com', salary => 95_000,
        });
        $dbh->commit();
    }
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'INSERT'), 1, 'Dept INSERT');
    is($CDC->count_events('App::Schema',
        table => 'employees', operation => 'INSERT'), 1, 'Emp INSERT in same txn');
};

subtest 'Constraint violation – no CDC event' => sub {
    plan tests => 2;
    clean_tables();
    App::Schema->table('Employee')->insert({
        first_name => 'Eve', last_name => 'X',
        email => 'eve@example.com', salary => 50_000,
    });
    $CDC->clear_events('App::Schema');

    my $failed = 0;
    try {
        App::Schema->table('Employee')->insert({
            first_name => 'Eve2', last_name => 'Y',
            email => 'eve@example.com', salary => 60_000,
        });
    } catch { $failed = 1 };

    ok($failed, 'Duplicate rejected');
    is($CDC->count_events('App::Schema', table => 'employees'), 0,
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
                first_name => 'Good', last_name => 'Row',
                email => 'good@example.com', salary => 50_000,
            });
            App::Schema->table('Employee')->insert({
                first_name => 'Good', last_name => 'Row',
                email => 'good@example.com', salary => 60_000,
            });
            $dbh->commit();
        } catch { $caught = 1; $dbh->rollback() };
    }
    ok($caught, 'Duplicate caught in txn');
    is($CDC->count_events('App::Schema', table => 'employees'), 0, 'Zero events');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM employees');
    is($n, 0, 'Zero rows');
};

# ===============================================================
# SECTION 4: CLASS-METHOD UPDATE / DELETE
# ===============================================================

subtest 'Class-method UPDATE – per-row old/new' => sub {
    plan tests => 5;
    clean_tables();
    my $dept_id = _insert_dept(name => 'ClassUpd', location => 'Bern');
    for my $i (1..3) {
        App::Schema->table('Employee')->insert({
            department_id => $dept_id,
            first_name => "CM$i", last_name => "L$i",
            email => "cm${i}\@example.com", salary => 50_000,
        });
    }
    $CDC->clear_events('App::Schema');

    App::Schema->table('Employee')->update(
        -set   => { salary => 55_000 },
        -where => { department_id => $dept_id },
    );

    is($CDC->count_events('App::Schema',
        table => 'employees', operation => 'UPDATE'), 3, '3 UPDATE events');
    my $pairs = $CDC->event_pairs('App::Schema', table => 'employees');
    is(scalar @$pairs, 3, 'Three pairs');
    my ($old, $new) = @{ $pairs->[0] };
    like($old->{SALARY}, qr/^50000/, 'old salary');
    like($new->{SALARY}, qr/^55000/, 'new salary');
    ok(defined $old->{FIRST_NAME}, 'Full snapshot in old_data');
};

subtest 'Class-method DELETE – per-row old_data' => sub {
    plan tests => 3;
    clean_tables();
    for my $i (1..2) {
        App::Schema->table('Department')->insert({
            name => "Del$i", location => 'X',
        });
    }
    $CDC->clear_events('App::Schema');

    App::Schema->table('Department')->delete(-where => { location => 'X' });

    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'DELETE'), 2, '2 DELETE events');
    my $events = $CDC->events_for('App::Schema',
        table => 'departments', operation => 'DELETE');
    my $old = _parse($events->[0]{old_data});
    ok(defined $old->{NAME}, 'old NAME present');
    ok(!defined $events->[0]{new_data}, 'new_data NULL');
};

subtest 'Class-method UPDATE – no matching rows' => sub {
    plan tests => 1;
    clean_tables();
    App::Schema->table('Department')->insert({
        name => 'NoMatch', location => 'X',
    });
    $CDC->clear_events('App::Schema');

    App::Schema->table('Department')->update(
        -set => { location => 'Y' }, -where => { name => 'NonExistent' },
    );

    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'UPDATE'), 0, 'Zero events');
};

# ===============================================================
# SECTION 5: DATA INTEGRITY & EDGE CASES
# ===============================================================

subtest 'NULL column values' => sub {
    plan tests => 3;
    clean_tables();
    App::Schema->table('Department')->insert({
        name => 'Ghost', location => undef,
    });
    my $ev  = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    my $new = _parse($ev->{new_data});
    ok($ev, 'INSERT with NULL captured');
    is($new->{NAME}, 'Ghost', 'NAME correct');
    ok(!defined $new->{LOCATION} || $new->{LOCATION} eq '',
        'NULL location captured');
};

subtest 'UPDATE preserves unchanged columns' => sub {
    plan tests => 3;
    clean_tables();
    App::Schema->table('Department')->insert({
        name => 'Stable', location => 'Bern',
    });
    my $dept = _fetch_dept({ name => 'Stable' });
    $CDC->clear_events('App::Schema');

    $dept->update({ location => 'Basel' });

    my $ev  = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'UPDATE');
    my $old = _parse($ev->{old_data});
    my $new = _parse($ev->{new_data});
    is($old->{NAME}, $new->{NAME}, 'NAME unchanged');
    is($old->{ID},   $new->{ID},   'PK unchanged');
    isnt($old->{LOCATION}, $new->{LOCATION}, 'LOCATION changed');
};

subtest 'Bulk INSERT – one event per row' => sub {
    plan tests => 1;
    clean_tables();
    my $dept_id = _insert_dept(name => 'Bulk', location => 'Bern');
    $CDC->clear_events('App::Schema');
    for my $i (1..5) {
        App::Schema->table('Employee')->insert({
            department_id => $dept_id,
            first_name => "U$i", last_name => "L$i",
            email => "u${i}\@example.com", salary => 50_000 + $i * 1_000,
        });
    }
    is($CDC->count_events('App::Schema',
        table => 'employees', operation => 'INSERT'), 5, '5 events');
};

subtest 'Cross-table FK tracking' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({
        name => 'CrossRef', location => 'Zug',
    });
    my $d_ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    ok($d_ev, 'Dept INSERT tracked');
    my $dept_id = _parse($d_ev->{new_data})->{ID};

    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name => 'Frank', last_name => 'Z',
        email => 'frank@example.com', salary => 80_000,
    });
    my $e_ev = $CDC->latest_event('App::Schema',
        table => 'employees', operation => 'INSERT');
    ok($e_ev, 'Emp INSERT tracked');
    my $emp = _parse($e_ev->{new_data});
    is($emp->{FIRST_NAME}, 'Frank', 'first_name captured');
    is($emp->{DEPARTMENT_ID}, $dept_id, 'FK captured');
};

subtest 'Special characters – accents, apostrophe' => sub {
    plan tests => 2;
    clean_tables();
    my $name = "R&D / O'Brien \x{2013} caf\x{e9}";
    App::Schema->table('Department')->insert({
        name => $name, location => "Neuch\x{e2}tel",
    });
    my $ev  = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    ok($ev, 'Special chars captured');
    my $new = _parse($ev->{new_data});
    is($new->{NAME}, $name, 'Round-trip correct');
};

subtest 'Event metadata' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({
        name => 'Meta', location => 'Bern',
    });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    ok($ev, 'Event present');
    ok($ev->{event_id} > 0, 'event_id positive');
    ok(defined $ev->{event_time}, 'event_time set');
    is($ev->{table_name}, 'DEPARTMENTS', 'table_name upper-case');
};

subtest 'Event ordering' => sub {
    plan tests => 2;
    clean_tables();
    for my $i (1..3) {
        App::Schema->table('Department')->insert({
            name => "Ord$i", location => 'X',
        });
    }
    my $events = $CDC->events_for('App::Schema', table => 'departments');
    is(scalar @$events, 3, 'Three events');
    my $ordered = 1;
    for my $i (1..$#$events) {
        $ordered = 0 if $events->[$i]{event_id} <= $events->[$i-1]{event_id};
    }
    ok($ordered, 'Ascending event_id order');
};

# ===============================================================
# SECTION 6: HELPER METHODS
# ===============================================================

subtest 'event_pairs() helper' => sub {
    plan tests => 3;
    clean_tables();
    my $dept_id = _insert_dept(name => 'Pair', location => 'Bern');
    for my $i (1..2) {
        App::Schema->table('Employee')->insert({
            department_id => $dept_id,
            first_name => "P$i", last_name => "L$i",
            email => "pair${i}\@example.com", salary => 60_000,
        });
    }
    $CDC->clear_events('App::Schema');
    for my $emp (@{ App::Schema->table('Employee')
            ->select(-where => { department_id => $dept_id }) }) {
        $emp->update({ salary => 65_000 });
    }
    my $pairs = $CDC->event_pairs('App::Schema', table => 'employees');
    is(scalar @$pairs, 2, 'Two pairs');
    my ($old, $new) = @{ $pairs->[0] };
    like($old->{SALARY}, qr/^60000/, 'old salary 60000');
    like($new->{SALARY}, qr/^65000/, 'new salary 65000');
};

subtest 'clear_events_for – selective' => sub {
    plan tests => 2;
    clean_tables();
    my $dept_id = _insert_dept(name => 'Sel', location => 'X');
    App::Schema->table('Employee')->insert({
        department_id => $dept_id,
        first_name => 'S', last_name => 'T',
        email => 's@example.com', salary => 50_000,
    });
    $CDC->clear_events_for('App::Schema', table => 'departments');
    is($CDC->count_events('App::Schema', table => 'departments'), 0, 'Dept cleared');
    is($CDC->count_events('App::Schema', table => 'employees'), 1, 'Emp preserved');
};

# ===============================================================
# SECTION 7: PLUGIN-SPECIFIC FEATURES
# ===============================================================

subtest 'Callback handler receives event envelope' => sub {
    plan tests => 5;
    clean_tables();
    @callback_events = ();

    App::Schema->table('Department')->insert({
        name => 'CbTest', location => 'Geneva',
    });

    ok(scalar @callback_events >= 1, 'Callback handler fired');
    my $ev = $callback_events[-1];
    ok(defined $ev->{event_id},    'event_id present');
    ok(defined $ev->{occurred_at}, 'occurred_at present');
    is($ev->{operation}, 'INSERT', 'operation correct');
    is($ev->{table_name}, 'DEPARTMENTS', 'table_name correct');
};

subtest 'Event envelope includes changed_columns for UPDATE' => sub {
    plan tests => 2;
    clean_tables();
    @callback_events = ();

    App::Schema->table('Department')->insert({
        name => 'Diff', location => 'Bern',
    });
    my $dept = _fetch_dept({ name => 'Diff' });
    @callback_events = ();

    $dept->update({ location => 'Basel' });

    my @updates = grep { $_->{operation} eq 'UPDATE' } @callback_events;
    ok(scalar @updates >= 1, 'UPDATE callback fired');
    my $changed = $updates[-1]{changed_columns};
    ok((grep { $_ eq 'LOCATION' } @$changed), 'LOCATION in changed_columns');
};

subtest 'Raw DBI bypass – not captured' => sub {
    plan tests => 1;
    clean_tables();
    $dbh->do(q{INSERT INTO departments(name,location) VALUES(?,?)},
        undef, 'RawDBI', 'Nowhere');
    is($CDC->count_events('App::Schema', table => 'departments'), 0,
        'Raw DBI not captured (by design)');
};

# ---------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------
clean_tables();
$dbh->disconnect();
done_testing();
