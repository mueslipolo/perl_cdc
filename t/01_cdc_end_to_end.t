#!/usr/bin/env perl
# =============================================================
# t/01_cdc_end_to_end.t
#
# Comprehensive test suite for DBIx::DataModel::Plugin::CDC.
#
# Sections:
#   1. Infrastructure
#   2. Basic CRUD capture
#   3. Transaction safety & atomicity
#   4. Class-method update/delete
#   5. Data integrity & edge cases
#   6. Query helpers
#   7. Plugin: handlers, envelope, Multi, error policies
#   8. Performance benchmarks
#   9. Design trade-offs (documented)
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
use Time::HiRes qw(gettimeofday tv_interval);

use lib 'lib';
use App::Schema;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Event;
use DBIx::DataModel::Plugin::CDC::Handler::DBI;
use DBIx::DataModel::Plugin::CDC::Handler::Callback;
use DBIx::DataModel::Plugin::CDC::Handler::Log;
use DBIx::DataModel::Plugin::CDC::Handler::Multi;

my $JSON_ENCODE = Cpanel::JSON::XS->new->utf8->canonical->allow_nonref;
my $JSON_DECODE = Cpanel::JSON::XS->new->canonical->allow_nonref;

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

# Callback event collector
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
}

sub _insert_dept {
    my (%args) = @_;
    App::Schema->table('Department')->insert({
        name     => $args{name}     // 'TestDept',
        location => $args{location} // 'Geneva',
    });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT')
        or die 'No INSERT event for department';
    return _parse($ev->{new_data})->{ID};
}

sub _fetch_dept {
    my ($where) = @_;
    my ($r) = @{ App::Schema->table('Department')->select(-where => $where) };
    return $r;
}

sub _fetch_emp {
    my ($where) = @_;
    my ($r) = @{ App::Schema->table('Employee')->select(-where => $where) };
    return $r;
}

sub _parse {
    my ($json_str) = @_;
    return undef unless defined $json_str;
    return $JSON_DECODE->decode($json_str);
}

# ===============================================================
# 1. INFRASTRUCTURE
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
# 2. BASIC CRUD CAPTURE
# ===============================================================

subtest 'INSERT – Department via ORM' => sub {
    plan tests => 6;
    clean_tables();
    App::Schema->table('Department')->insert({
        name => 'Engineering', location => 'Geneva',
    });
    my $events = $CDC->events_for('App::Schema',
        table => 'departments', operation => 'INSERT');
    is(scalar @$events, 1,       'Exactly one INSERT event');
    is($events->[0]{operation}, 'INSERT', 'operation is INSERT');
    ok(!defined $events->[0]{old_data},   'old_data is NULL');
    ok(defined  $events->[0]{new_data},   'new_data populated');
    my $new = _parse($events->[0]{new_data});
    is($new->{NAME},     'Engineering', 'NAME correct');
    is($new->{LOCATION}, 'Geneva',      'LOCATION correct');
};

subtest 'INSERT – Employee via ORM with FK' => sub {
    plan tests => 4;
    clean_tables();
    my $dept_id = _insert_dept(name => 'HR', location => 'Zurich');
    $CDC->clear_events('App::Schema');
    App::Schema->table('Employee')->insert({
        department_id => $dept_id, first_name => 'Alice',
        last_name => 'Dupont', email => 'alice@example.com', salary => 90_000,
    });
    my $ev  = $CDC->latest_event('App::Schema',
        table => 'employees', operation => 'INSERT');
    ok($ev, 'INSERT captured');
    my $new = _parse($ev->{new_data});
    is($new->{FIRST_NAME},    'Alice',             'first_name');
    is($new->{EMAIL},         'alice@example.com', 'email');
    is($new->{DEPARTMENT_ID}, $dept_id,            'FK');
};

subtest 'UPDATE – instance method' => sub {
    plan tests => 3;
    clean_tables();
    my $dept_id = _insert_dept(name => 'Legal', location => 'Lausanne');
    App::Schema->table('Employee')->insert({
        department_id => $dept_id, first_name => 'Bob',
        last_name => 'Martin', email => 'bob@example.com', salary => 75_000,
    });
    $CDC->clear_events('App::Schema');
    my $emp = _fetch_emp({ email => 'bob@example.com' });
    $emp->update({ salary => 80_000 });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'employees', operation => 'UPDATE');
    ok($ev, 'UPDATE captured');
    like(_parse($ev->{old_data})->{SALARY}, qr/^75000/, 'old salary');
    like(_parse($ev->{new_data})->{SALARY}, qr/^80000/, 'new salary');
};

subtest 'DELETE – instance method' => sub {
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
    ok($ev, 'DELETE captured');
    is($ev->{operation}, 'DELETE', 'operation');
    ok(!defined $ev->{new_data}, 'new_data NULL');
    is(_parse($ev->{old_data})->{NAME}, 'Marketing', 'old NAME');
};

# ===============================================================
# 3. TRANSACTION SAFETY & ATOMICITY
# ===============================================================

subtest 'ROLLBACK – discards DML and CDC events' => sub {
    plan tests => 2;
    clean_tables();
    { local $dbh->{AutoCommit} = 0;
      try { App::Schema->table('Department')->insert({
          name => 'Rollback', location => 'X' });
        $dbh->rollback();
      } catch { $dbh->rollback() };
    }
    is($CDC->count_events('App::Schema', table => 'departments'), 0, 'Zero events');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM departments');
    is($n, 0, 'Zero rows');
};

subtest 'COMMIT – multi-statement transaction' => sub {
    plan tests => 3;
    clean_tables();
    { local $dbh->{AutoCommit} = 0;
      App::Schema->table('Department')->insert({ name => 'Ops1', location => 'Z' });
      App::Schema->table('Department')->insert({ name => 'Ops2', location => 'Z' });
      $dbh->commit();
    }
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'INSERT'), 2, '2 INSERTs');
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'UPDATE'), 0, '0 UPDATEs');
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'DELETE'), 0, '0 DELETEs');
};

subtest 'AutoCommit ON – DML + CDC event atomic' => sub {
    plan tests => 2;
    clean_tables();
    ok($dbh->{AutoCommit}, 'AutoCommit ON');
    App::Schema->table('Department')->insert({ name => 'Atomic', location => 'B' });
    my ($n) = $dbh->selectrow_array("SELECT COUNT(*) FROM departments WHERE name='Atomic'");
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'INSERT'),
        $n, 'CDC count matches row count');
};

subtest 'Full lifecycle in single txn' => sub {
    plan tests => 4;
    clean_tables();
    { local $dbh->{AutoCommit} = 0;
      App::Schema->table('Department')->insert({ name => 'Life', location => 'A' });
      my $d = _fetch_dept({ name => 'Life' });
      $d->update({ location => 'B' });
      $d->delete();
      $dbh->commit();
    }
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'INSERT'), 1, 'INSERT');
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'UPDATE'), 1, 'UPDATE');
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'DELETE'), 1, 'DELETE');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM departments');
    is($n, 0, 'Deleted');
};

subtest 'Partial rollback on error' => sub {
    plan tests => 2;
    clean_tables();
    my $caught = 0;
    { local $dbh->{AutoCommit} = 0;
      try {
        App::Schema->table('Department')->insert({ name => 'OK', location => 'X' });
        App::Schema->table('Department')->insert({ name => 'A' x 200, location => 'X' });
        $dbh->commit();
      } catch { $caught = 1; $dbh->rollback() };
    }
    ok($caught, 'Exception caught');
    is($CDC->count_events('App::Schema', table => 'departments'), 0, 'Zero events');
};

subtest 'Interleaved tables in single txn' => sub {
    plan tests => 2;
    clean_tables();
    { local $dbh->{AutoCommit} = 0;
      App::Schema->table('Department')->insert({ name => 'X', location => 'Z' });
      my $d = _fetch_dept({ name => 'X' });
      App::Schema->table('Employee')->insert({
          department_id => $d->{id}, first_name => 'Z', last_name => 'K',
          email => 'zk@example.com', salary => 95_000 });
      $dbh->commit();
    }
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'INSERT'), 1, 'Dept');
    is($CDC->count_events('App::Schema', table => 'employees', operation => 'INSERT'), 1, 'Emp');
};

subtest 'Constraint violation – no CDC event' => sub {
    plan tests => 2;
    clean_tables();
    App::Schema->table('Employee')->insert({
        first_name => 'Eve', last_name => 'X',
        email => 'eve@example.com', salary => 50_000 });
    $CDC->clear_events('App::Schema');
    my $failed = 0;
    try { App::Schema->table('Employee')->insert({
        first_name => 'Eve2', last_name => 'Y',
        email => 'eve@example.com', salary => 60_000 });
    } catch { $failed = 1 };
    ok($failed, 'Duplicate rejected');
    is($CDC->count_events('App::Schema', table => 'employees'), 0, 'No event');
};

subtest 'Constraint violation in txn – full rollback' => sub {
    plan tests => 3;
    clean_tables();
    my $caught = 0;
    { local $dbh->{AutoCommit} = 0;
      try {
        App::Schema->table('Employee')->insert({
            first_name => 'A', last_name => 'B',
            email => 'dup@example.com', salary => 50_000 });
        App::Schema->table('Employee')->insert({
            first_name => 'C', last_name => 'D',
            email => 'dup@example.com', salary => 60_000 });
        $dbh->commit();
      } catch { $caught = 1; $dbh->rollback() };
    }
    ok($caught, 'Caught');
    is($CDC->count_events('App::Schema', table => 'employees'), 0, 'Zero events');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM employees');
    is($n, 0, 'Zero rows');
};

# ===============================================================
# 4. CLASS-METHOD UPDATE / DELETE
# ===============================================================

subtest 'Class-method UPDATE – per-row' => sub {
    plan tests => 5;
    clean_tables();
    my $did = _insert_dept(name => 'CU', location => 'B');
    for my $i (1..3) {
        App::Schema->table('Employee')->insert({
            department_id => $did, first_name => "C$i", last_name => "L$i",
            email => "c${i}\@example.com", salary => 50_000 });
    }
    $CDC->clear_events('App::Schema');
    App::Schema->table('Employee')->update(
        -set => { salary => 55_000 }, -where => { department_id => $did });
    is($CDC->count_events('App::Schema', table => 'employees', operation => 'UPDATE'), 3, '3 events');
    my $pairs = $CDC->event_pairs('App::Schema', table => 'employees');
    is(scalar @$pairs, 3, '3 pairs');
    like($pairs->[0][0]{SALARY}, qr/^50000/, 'old');
    like($pairs->[0][1]{SALARY}, qr/^55000/, 'new');
    ok(defined $pairs->[0][0]{FIRST_NAME}, 'Full snapshot');
};

subtest 'Class-method DELETE – per-row' => sub {
    plan tests => 3;
    clean_tables();
    App::Schema->table('Department')->insert({ name => "D$_", location => 'X' }) for 1..2;
    $CDC->clear_events('App::Schema');
    App::Schema->table('Department')->delete(-where => { location => 'X' });
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'DELETE'), 2, '2 events');
    my $ev = ($CDC->events_for('App::Schema', table => 'departments', operation => 'DELETE'))->[0];
    ok(defined _parse($ev->{old_data})->{NAME}, 'old NAME');
    ok(!defined $ev->{new_data}, 'new_data NULL');
};

subtest 'Class-method UPDATE – no matching rows' => sub {
    plan tests => 1;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'X', location => 'X' });
    $CDC->clear_events('App::Schema');
    App::Schema->table('Department')->update(
        -set => { location => 'Y' }, -where => { name => 'NOPE' });
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'UPDATE'), 0, 'Zero');
};

# ===============================================================
# 5. DATA INTEGRITY & EDGE CASES
# ===============================================================

subtest 'NULL column handling' => sub {
    plan tests => 2;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'Ghost', location => undef });
    my $new = _parse($CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT')->{new_data});
    is($new->{NAME}, 'Ghost', 'NAME');
    ok(!defined $new->{LOCATION} || $new->{LOCATION} eq '', 'NULL location');
};

subtest 'UPDATE preserves unchanged columns' => sub {
    plan tests => 3;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'Stable', location => 'B' });
    my $d = _fetch_dept({ name => 'Stable' });
    $CDC->clear_events('App::Schema');
    $d->update({ location => 'C' });
    my $ev = $CDC->latest_event('App::Schema', table => 'departments', operation => 'UPDATE');
    my $old = _parse($ev->{old_data});
    my $new = _parse($ev->{new_data});
    is($old->{NAME}, $new->{NAME}, 'NAME same');
    is($old->{ID},   $new->{ID},   'PK same');
    isnt($old->{LOCATION}, $new->{LOCATION}, 'LOCATION differs');
};

subtest 'Bulk INSERT – 5 rows → 5 events' => sub {
    plan tests => 1;
    clean_tables();
    my $did = _insert_dept(name => 'Bulk', location => 'B');
    $CDC->clear_events('App::Schema');
    for my $i (1..5) {
        App::Schema->table('Employee')->insert({
            department_id => $did, first_name => "U$i", last_name => "L$i",
            email => "u${i}\@example.com", salary => 50_000 + $i * 1000 });
    }
    is($CDC->count_events('App::Schema', table => 'employees', operation => 'INSERT'), 5, '5');
};

subtest 'Cross-table FK tracking' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'Cross', location => 'Z' });
    my $did = _parse($CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT')->{new_data})->{ID};
    ok($did, 'Dept ID');
    App::Schema->table('Employee')->insert({
        department_id => $did, first_name => 'F', last_name => 'Z',
        email => 'f@example.com', salary => 80_000 });
    my $emp = _parse($CDC->latest_event('App::Schema',
        table => 'employees', operation => 'INSERT')->{new_data});
    ok($emp, 'Emp event');
    is($emp->{FIRST_NAME}, 'F', 'first_name');
    is($emp->{DEPARTMENT_ID}, $did, 'FK');
};

subtest 'Special characters – accents, apostrophe, en-dash' => sub {
    plan tests => 2;
    clean_tables();
    my $name = "R&D / O'Brien \x{2013} caf\x{e9}";
    App::Schema->table('Department')->insert({
        name => $name, location => "Neuch\x{e2}tel" });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    ok($ev, 'Captured');
    is(_parse($ev->{new_data})->{NAME}, $name, 'Round-trip');
};

subtest 'Empty string vs NULL' => sub {
    plan tests => 2;
    clean_tables();
    # Oracle converts '' to NULL, so both should behave the same
    App::Schema->table('Department')->insert({ name => 'EmptyTest', location => '' });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    my $new = _parse($ev->{new_data});
    is($new->{NAME}, 'EmptyTest', 'NAME');
    # Oracle treats '' as NULL — accept either
    ok(!defined $new->{LOCATION} || $new->{LOCATION} eq '',
        'Empty string treated as NULL by Oracle');
};

subtest 'Multiple updates on same row – full history' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'History', location => 'A' });
    my $d = _fetch_dept({ name => 'History' });
    $CDC->clear_events('App::Schema');
    $d->update({ location => 'B' });
    $d->update({ location => 'C' });
    $d->update({ location => 'D' });
    my $events = $CDC->events_for('App::Schema',
        table => 'departments', operation => 'UPDATE');
    is(scalar @$events, 3, '3 UPDATE events');
    is(_parse($events->[0]{new_data})->{LOCATION}, 'B', 'First → B');
    is(_parse($events->[1]{new_data})->{LOCATION}, 'C', 'Second → C');
    is(_parse($events->[2]{new_data})->{LOCATION}, 'D', 'Third → D');
};

subtest 'JSON serialization – structured data round-trip' => sub {
    plan tests => 3;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'JSON', location => 'Geneva' });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    # Verify it's valid JSON
    my $data;
    lives_ok { $data = _parse($ev->{new_data}) } 'new_data is valid JSON';
    is(ref $data, 'HASH', 'Decodes to hashref');
    ok(exists $data->{NAME} && exists $data->{LOCATION}, 'Expected keys present');
};

# ===============================================================
# 6. METADATA & QUERY HELPERS
# ===============================================================

subtest 'Event metadata' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'Meta', location => 'B' });
    my $ev = $CDC->latest_event('App::Schema',
        table => 'departments', operation => 'INSERT');
    ok($ev, 'Event present');
    ok($ev->{event_id} > 0, 'event_id positive');
    ok(defined $ev->{event_time}, 'event_time set');
    is($ev->{table_name}, 'DEPARTMENTS', 'table_name upper-case');
};

subtest 'Event ordering – ascending event_id' => sub {
    plan tests => 2;
    clean_tables();
    App::Schema->table('Department')->insert({ name => "O$_", location => 'X' }) for 1..3;
    my $events = $CDC->events_for('App::Schema', table => 'departments');
    is(scalar @$events, 3, 'Three events');
    my $ordered = 1;
    for my $i (1..$#$events) {
        $ordered = 0 if $events->[$i]{event_id} <= $events->[$i-1]{event_id};
    }
    ok($ordered, 'Ascending');
};

subtest 'event_pairs() helper' => sub {
    plan tests => 3;
    clean_tables();
    my $did = _insert_dept(name => 'Pair', location => 'B');
    App::Schema->table('Employee')->insert({
        department_id => $did, first_name => "P$_", last_name => "L$_",
        email => "p${_}\@example.com", salary => 60_000 }) for 1..2;
    $CDC->clear_events('App::Schema');
    for my $e (@{ App::Schema->table('Employee')
            ->select(-where => { department_id => $did }) }) {
        $e->update({ salary => 65_000 });
    }
    my $pairs = $CDC->event_pairs('App::Schema', table => 'employees');
    is(scalar @$pairs, 2, 'Two pairs');
    like($pairs->[0][0]{SALARY}, qr/^60000/, 'old 60000');
    like($pairs->[0][1]{SALARY}, qr/^65000/, 'new 65000');
};

subtest 'count_events filters by operation' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'Count', location => 'B' });
    my $d = _fetch_dept({ name => 'Count' });
    $d->update({ location => 'C' });
    is($CDC->count_events('App::Schema', table => 'departments'), 2, 'Total 2');
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'INSERT'), 1, 'INSERT 1');
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'UPDATE'), 1, 'UPDATE 1');
    is($CDC->count_events('App::Schema', table => 'departments', operation => 'DELETE'), 0, 'DELETE 0');
};

subtest 'clear_events_for – selective' => sub {
    plan tests => 2;
    clean_tables();
    my $did = _insert_dept(name => 'Sel', location => 'X');
    App::Schema->table('Employee')->insert({
        department_id => $did, first_name => 'S', last_name => 'T',
        email => 's@example.com', salary => 50_000 });
    $CDC->clear_events_for('App::Schema', table => 'departments');
    is($CDC->count_events('App::Schema', table => 'departments'), 0, 'Dept cleared');
    is($CDC->count_events('App::Schema', table => 'employees'), 1, 'Emp preserved');
};

# ===============================================================
# 7. PLUGIN: HANDLERS, ENVELOPE, MULTI, ERROR POLICIES
# ===============================================================

subtest 'Callback receives correct event envelope' => sub {
    plan tests => 7;
    clean_tables();
    @callback_events = ();
    App::Schema->table('Department')->insert({ name => 'Env', location => 'G' });
    ok(@callback_events >= 1, 'Callback fired');
    my $ev = $callback_events[-1];
    ok(defined $ev->{event_id},      'event_id');
    ok(defined $ev->{occurred_at},   'occurred_at');
    is($ev->{operation},  'INSERT',     'operation');
    is($ev->{table_name}, 'DEPARTMENTS', 'table_name');
    is($ev->{schema_name}, 'App::Schema', 'schema_name');
    is(ref $ev->{new_data}, 'HASH', 'new_data is hashref');
};

subtest 'Callback receives changed_columns for UPDATE' => sub {
    plan tests => 3;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'CC', location => 'B' });
    my $d = _fetch_dept({ name => 'CC' });
    @callback_events = ();
    $d->update({ location => 'C' });
    my @upd = grep { $_->{operation} eq 'UPDATE' } @callback_events;
    ok(@upd >= 1, 'UPDATE callback');
    my $changed = $upd[-1]{changed_columns};
    ok(ref $changed eq 'ARRAY', 'changed_columns is arrayref');
    ok((grep { $_ eq 'LOCATION' } @$changed), 'LOCATION in changed_columns');
};

subtest 'Callback receives old_data and new_data for UPDATE' => sub {
    plan tests => 4;
    clean_tables();
    App::Schema->table('Department')->insert({ name => 'OldNew', location => 'A' });
    my $d = _fetch_dept({ name => 'OldNew' });
    @callback_events = ();
    $d->update({ location => 'B' });
    my @upd = grep { $_->{operation} eq 'UPDATE' } @callback_events;
    my $ev = $upd[-1];
    is(ref $ev->{old_data}, 'HASH', 'old_data is hashref');
    is(ref $ev->{new_data}, 'HASH', 'new_data is hashref');
    is($ev->{old_data}{LOCATION}, 'A', 'old LOCATION');
    is($ev->{new_data}{LOCATION}, 'B', 'new LOCATION');
};

subtest 'INSERT callback – old_data undef, DELETE callback – new_data undef' => sub {
    plan tests => 4;
    clean_tables();
    @callback_events = ();
    App::Schema->table('Department')->insert({ name => 'Null', location => 'X' });
    my @ins = grep { $_->{operation} eq 'INSERT' } @callback_events;
    ok(!defined $ins[-1]{old_data}, 'INSERT: old_data undef');
    ok(defined  $ins[-1]{new_data}, 'INSERT: new_data defined');

    my $d = _fetch_dept({ name => 'Null' });
    @callback_events = ();
    $d->delete();
    my @del = grep { $_->{operation} eq 'DELETE' } @callback_events;
    ok(defined  $del[-1]{old_data}, 'DELETE: old_data defined');
    ok(!defined $del[-1]{new_data}, 'DELETE: new_data undef');
};

subtest 'Event::build – generates unique IDs' => sub {
    plan tests => 2;
    my $e1 = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X', table_name => 'T', operation => 'INSERT',
        old_data => undef, new_data => { A => 1 });
    my $e2 = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X', table_name => 'T', operation => 'INSERT',
        old_data => undef, new_data => { A => 2 });
    ok(defined $e1->{event_id}, 'ID generated');
    isnt($e1->{event_id}, $e2->{event_id}, 'IDs are unique');
};

subtest 'Event::build – changed_columns only for UPDATE' => sub {
    plan tests => 3;
    my $ins = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X', table_name => 'T', operation => 'INSERT',
        old_data => undef, new_data => { A => 1 });
    ok(!defined $ins->{changed_columns}, 'INSERT: no changed_columns');

    my $del = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X', table_name => 'T', operation => 'DELETE',
        old_data => { A => 1 }, new_data => undef);
    ok(!defined $del->{changed_columns}, 'DELETE: no changed_columns');

    my $upd = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X', table_name => 'T', operation => 'UPDATE',
        old_data => { A => 1, B => 2 }, new_data => { A => 1, B => 3 });
    is_deeply($upd->{changed_columns}, ['B'], 'UPDATE: only B changed');
};

subtest 'Multi handler – DBI + Callback both fire' => sub {
    plan tests => 3;
    clean_tables();

    my @multi_events;
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::DBI->new(table_name => 'cdc_events'),
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { push @multi_events, $_[0] },
                phase    => 'in_transaction',
            ),
        ],
    );

    # Temporarily replace handlers to test Multi
    my $cfg = $CDC->config_for('App::Schema');
    my $orig_handlers = $cfg->{handlers};
    $cfg->{handlers} = [$multi];

    App::Schema->table('Department')->insert({ name => 'Multi', location => 'X' });

    is($CDC->count_events('App::Schema', table => 'departments', operation => 'INSERT'),
        1, 'DBI handler wrote event');
    ok(@multi_events >= 1, 'Callback handler also fired');
    is($multi_events[-1]{operation}, 'INSERT', 'Callback got INSERT');

    # Restore
    $cfg->{handlers} = $orig_handlers;
};

subtest 'Handler error policy: warn – DML succeeds' => sub {
    plan tests => 2;
    clean_tables();

    my $failing_cb = DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
        on_event => sub { die "Intentional failure" },
        phase    => 'in_transaction',
        on_error => 'warn',
    );
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            $dbi_handler,
            $failing_cb,
        ],
    );

    my $cfg = $CDC->config_for('App::Schema');
    my $orig = $cfg->{handlers};
    $cfg->{handlers} = [$multi];

    # The failing handler should warn but not abort the DML
    my $warned = 0;
    local $SIG{__WARN__} = sub { $warned++ if $_[0] =~ /Intentional/ };
    lives_ok {
        App::Schema->table('Department')->insert({ name => 'Warn', location => 'X' });
    } 'DML succeeds despite handler failure';
    ok($warned, 'Warning was emitted');

    $cfg->{handlers} = $orig;
};

subtest 'Handler error policy: abort – DML rolls back' => sub {
    plan tests => 2;
    clean_tables();

    my $failing_cb = DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
        on_event => sub { die "Abort!" },
        phase    => 'in_transaction',
        on_error => 'abort',
    );
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [$dbi_handler, $failing_cb],
    );

    my $cfg = $CDC->config_for('App::Schema');
    my $orig = $cfg->{handlers};
    $cfg->{handlers} = [$multi];

    my $died = 0;
    try {
        App::Schema->table('Department')->insert({ name => 'Abort', location => 'X' });
    } catch { $died = 1 };

    ok($died, 'DML aborted');
    my ($n) = $dbh->selectrow_array("SELECT COUNT(*) FROM departments WHERE name='Abort'");
    is($n, 0, 'Row not committed');

    $cfg->{handlers} = $orig;
};

# ===============================================================
# 8. PERFORMANCE BENCHMARKS
# ===============================================================

subtest 'Performance – INSERT throughput' => sub {
    plan tests => 3;
    clean_tables();
    my $N = $ENV{CDC_PERF_N} || 100;

    # Baseline: raw DBI inserts (no CDC)
    my $t0 = [gettimeofday];
    for my $i (1..$N) {
        $dbh->do(q{INSERT INTO departments(name, location) VALUES(?, ?)},
            undef, "raw_$i", 'bench');
    }
    my $raw_elapsed = tv_interval($t0);
    my $raw_rate = $N / ($raw_elapsed || 0.001);

    # Cleanup
    $dbh->do("DELETE FROM departments");
    $CDC->clear_events('App::Schema');

    # CDC: ORM inserts with CDC enabled
    $t0 = [gettimeofday];
    for my $i (1..$N) {
        App::Schema->table('Department')->insert({
            name => "cdc_$i", location => 'bench',
        });
    }
    my $cdc_elapsed = tv_interval($t0);
    my $cdc_rate = $N / ($cdc_elapsed || 0.001);

    my $overhead_pct = (($cdc_elapsed - $raw_elapsed) / ($raw_elapsed || 0.001)) * 100;

    # Report
    diag sprintf "INSERT benchmark (N=%d):", $N;
    diag sprintf "  Raw DBI:  %.1f ops/s (%.3fs total)", $raw_rate, $raw_elapsed;
    diag sprintf "  CDC ORM:  %.1f ops/s (%.3fs total)", $cdc_rate, $cdc_elapsed;
    diag sprintf "  Overhead: %.1f%%", $overhead_pct;

    ok($raw_rate > 0, "Raw DBI rate: ${\sprintf '%.0f', $raw_rate} ops/s");
    ok($cdc_rate > 0, "CDC ORM rate: ${\sprintf '%.0f', $cdc_rate} ops/s");

    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'INSERT'), $N,
        "All $N CDC events captured");
};

subtest 'Performance – UPDATE throughput' => sub {
    plan tests => 2;
    clean_tables();
    my $N = $ENV{CDC_PERF_N} || 100;

    # Setup: insert N rows
    for my $i (1..$N) {
        App::Schema->table('Department')->insert({
            name => "upd_$i", location => 'before',
        });
    }
    $CDC->clear_events('App::Schema');

    # Fetch all rows, then update each
    my $rows = App::Schema->table('Department')
        ->select(-where => { location => 'before' });

    my $t0 = [gettimeofday];
    for my $row (@$rows) {
        $row->update({ location => 'after' });
    }
    my $elapsed = tv_interval($t0);
    my $rate = $N / ($elapsed || 0.001);

    diag sprintf "UPDATE benchmark (N=%d): %.1f ops/s (%.3fs)", $N, $rate, $elapsed;

    ok($rate > 0, "UPDATE rate: ${\sprintf '%.0f', $rate} ops/s");
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'UPDATE'), $N,
        "All $N UPDATE events captured");
};

subtest 'Performance – DELETE throughput' => sub {
    plan tests => 2;
    clean_tables();
    my $N = $ENV{CDC_PERF_N} || 100;

    for my $i (1..$N) {
        App::Schema->table('Department')->insert({
            name => "del_$i", location => 'gone',
        });
    }
    $CDC->clear_events('App::Schema');

    my $rows = App::Schema->table('Department')
        ->select(-where => { location => 'gone' });

    my $t0 = [gettimeofday];
    for my $row (@$rows) {
        $row->delete();
    }
    my $elapsed = tv_interval($t0);
    my $rate = $N / ($elapsed || 0.001);

    diag sprintf "DELETE benchmark (N=%d): %.1f ops/s (%.3fs)", $N, $rate, $elapsed;

    ok($rate > 0, "DELETE rate: ${\sprintf '%.0f', $rate} ops/s");
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'DELETE'), $N,
        "All $N DELETE events captured");
};

subtest 'Performance – batch INSERT in transaction' => sub {
    plan tests => 2;
    clean_tables();
    my $N = $ENV{CDC_PERF_N} || 100;

    my $t0 = [gettimeofday];
    {
        local $dbh->{AutoCommit} = 0;
        for my $i (1..$N) {
            App::Schema->table('Department')->insert({
                name => "txn_$i", location => 'batch',
            });
        }
        $dbh->commit();
    }
    my $elapsed = tv_interval($t0);
    my $rate = $N / ($elapsed || 0.001);

    diag sprintf "Batch INSERT in txn (N=%d): %.1f ops/s (%.3fs)", $N, $rate, $elapsed;

    ok($rate > 0, "Batch rate: ${\sprintf '%.0f', $rate} ops/s");
    is($CDC->count_events('App::Schema',
        table => 'departments', operation => 'INSERT'), $N,
        "All $N events captured in batch");
};

# ===============================================================
# 9. DESIGN TRADE-OFFS (DOCUMENTED)
# ===============================================================

subtest 'Raw DBI bypass – not captured (by design)' => sub {
    plan tests => 1;
    clean_tables();
    $dbh->do(q{INSERT INTO departments(name,location) VALUES(?,?)},
        undef, 'RawDBI', 'Nowhere');
    is($CDC->count_events('App::Schema', table => 'departments'), 0,
        'Raw DBI not captured');
};

# ---------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------
clean_tables();
$dbh->disconnect();
done_testing();
