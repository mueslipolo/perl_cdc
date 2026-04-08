#!/usr/bin/env perl
# =============================================================
# Oracle backend for the shared CDC e2e test suite.
#
# Runs the common suite (shared with SQLite) plus Oracle-specific
# tests: transaction semantics, constraints, performance benchmarks,
# multi-table operations, and design trade-offs.
#
# Requires: Oracle DB (via Docker), env vars ORACLE_DSN/USER/PASS.
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

use FindBin;
use lib "$FindBin::Bin/../../../t/lib";
use lib "$FindBin::Bin/../../../lib";
use lib "$FindBin::Bin/../lib";

use CDCTestSuite::Schema;
use CDCTestSuite;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Event;

my $JSON_DECODE = Cpanel::JSON::XS->new->canonical->allow_nonref;
my $CDC = 'DBIx::DataModel::Plugin::CDC';

# ---------------------------------------------------------------
# Oracle connection
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

CDCTestSuite::Schema->dbh($dbh);

# ---------------------------------------------------------------
# Shared suite
# ---------------------------------------------------------------
my $suite = CDCTestSuite->new(dbh => $dbh);
$suite->setup_cdc(capture_old => 1);

# ---------------------------------------------------------------
# Helpers (Oracle-specific)
# ---------------------------------------------------------------
sub _parse {
    my ($json_str) = @_;
    return undef unless defined $json_str;
    return $JSON_DECODE->decode($json_str);
}

sub _fetch_dept {
    my ($where) = @_;
    my ($r) = @{ CDCTestSuite::Schema->table('Department')
        ->select(-where => $where) };
    return $r;
}

sub _fetch_emp {
    my ($where) = @_;
    my ($r) = @{ CDCTestSuite::Schema->table('Employee')
        ->select(-where => $where) };
    return $r;
}

sub _with_cdc_disabled {
    my ($code) = @_;
    my $cfg = $CDC->config_for('CDCTestSuite::Schema');
    my $saved = $cfg->{tracked};
    $cfg->{tracked} = {};
    my @result = $code->();
    $cfg->{tracked} = $saved;
    return @result;
}

# ===============================================================
# 1. INFRASTRUCTURE (Oracle-specific)
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
# 2. COMMON TEST SUITE (shared with SQLite)
# ===============================================================

$suite->run_common_suite;

# ===============================================================
# 3. ORACLE-SPECIFIC: TRANSACTION SEMANTICS
# ===============================================================

subtest 'ROLLBACK – discards DML and CDC events' => sub {
    plan tests => 2;
    $suite->clean;
    { local $dbh->{AutoCommit} = 0;
      try {
        CDCTestSuite::Schema->table('Department')->insert({
            name => 'Rollback', location => 'X',
        });
        $dbh->rollback();
      } catch { $dbh->rollback() };
    }
    is($CDC->count_events('CDCTestSuite::Schema', table => 'departments'), 0, 'Zero events');
    my ($n) = $dbh->selectrow_array('SELECT COUNT(*) FROM departments');
    is($n, 0, 'Zero rows');
};

subtest 'COMMIT – multi-statement transaction' => sub {
    plan tests => 3;
    $suite->clean;
    { local $dbh->{AutoCommit} = 0;
      CDCTestSuite::Schema->table('Department')->insert({
          name => 'Ops1', location => 'Z',
      });
      CDCTestSuite::Schema->table('Department')->insert({
          name => 'Ops2', location => 'Z',
      });
      $dbh->commit();
    }
    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'INSERT'), 2, '2 INSERTs');
    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'UPDATE'), 0, '0 UPDATEs');
    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'DELETE'), 0, '0 DELETEs');
};

subtest 'Constraint violation – no CDC event' => sub {
    plan tests => 2;
    $suite->clean;
    CDCTestSuite::Schema->table('Employee')->insert({
        first_name => 'Eve', last_name => 'X',
        email => 'eve@example.com', salary => 50_000,
    });
    $CDC->clear_events('CDCTestSuite::Schema');
    my $failed = 0;
    try {
        CDCTestSuite::Schema->table('Employee')->insert({
            first_name => 'Eve2', last_name => 'Y',
            email => 'eve@example.com', salary => 60_000,
        });
    } catch { $failed = 1 };
    ok($failed, 'Duplicate rejected');
    is($CDC->count_events('CDCTestSuite::Schema', table => 'employees'), 0, 'No event');
};

subtest 'Cross-table transaction – rollback undoes all events' => sub {
    plan tests => 3;
    $suite->clean;
    my $caught = 0;
    { local $dbh->{AutoCommit} = 0;
      try {
        CDCTestSuite::Schema->table('Department')->insert({
            name => 'TxnDept', location => 'Z',
        });
        my $dept = _fetch_dept({ name => 'TxnDept' });
        CDCTestSuite::Schema->table('Employee')->insert({
            department_id => $dept->{id}, first_name => 'T', last_name => 'X',
            email => 'tx@example.com', salary => 50_000,
        });
        die "Simulated error";
      } catch {
        $caught = 1;
        $dbh->rollback();
      };
    }
    ok($caught, 'Error caught');
    is($CDC->count_events('CDCTestSuite::Schema', table => 'departments'), 0,
        'Department events rolled back');
    is($CDC->count_events('CDCTestSuite::Schema', table => 'employees'), 0,
        'Employee events rolled back');
};

# ===============================================================
# 4. ORACLE-SPECIFIC: EDGE CASES
# ===============================================================

subtest 'Oracle – empty string is NULL' => sub {
    plan tests => 2;
    $suite->clean;
    CDCTestSuite::Schema->table('Department')->insert({
        name => 'EmptyTest', location => '',
    });
    my $ev = $CDC->latest_event('CDCTestSuite::Schema',
        table => 'departments', operation => 'INSERT');
    my $new = _parse($ev->{new_data});
    is($new->{NAME}, 'EmptyTest', 'NAME');
    ok(!defined $new->{LOCATION} || $new->{LOCATION} eq '',
        'Empty string treated as NULL by Oracle');
};

subtest 'Oracle – special characters round-trip' => sub {
    plan tests => 2;
    $suite->clean;
    my $name = "R&D / O'Brien \x{2013} caf\x{e9}";
    CDCTestSuite::Schema->table('Department')->insert({
        name => $name, location => "Neuch\x{e2}tel",
    });
    my $ev = $CDC->latest_event('CDCTestSuite::Schema',
        table => 'departments', operation => 'INSERT');
    ok($ev, 'Captured');
    is(_parse($ev->{new_data})->{NAME}, $name, 'Round-trip');
};

subtest 'Raw DBI bypass – not captured (by design)' => sub {
    plan tests => 1;
    $suite->clean;
    $dbh->do(q{INSERT INTO departments(name,location) VALUES(?,?)},
        undef, 'RawDBI', 'Nowhere');
    is($CDC->count_events('CDCTestSuite::Schema', table => 'departments'), 0,
        'Raw DBI not captured');
};

subtest 'Snapshot captures all columns, skips components' => sub {
    plan tests => 2;
    $suite->clean;
    my @cb;
    $CDC->setup('CDCTestSuite::Schema', tables => 'all', capture_old => 1)
        ->log_to_dbi('CDCTestSuite::Schema', 'cdc_events')
        ->on('CDCTestSuite::Schema', '*' => sub { push @cb, $_[0] },
            { phase => 'in_transaction' });

    CDCTestSuite::Schema->table('Department')->insert({
        name => 'SnapTest', location => 'Y',
    });

    my @ins = grep { $_->{operation} eq 'INSERT' } @cb;
    my $new = $ins[-1]{new_data};
    ok(defined $new->{NAME}, 'NAME present in snapshot');
    ok(!exists $new->{EMPLOYEES}, 'Component role name not in snapshot');

    $suite->setup_cdc(capture_old => 1);
};

# ===============================================================
# 5. ORACLE-SPECIFIC: PERFORMANCE BENCHMARKS
# ===============================================================

subtest 'Performance – INSERT: ORM vs ORM+CDC' => sub {
    plan tests => 4;
    my $N = $ENV{CDC_PERF_N} || 100;

    $suite->clean;
    my $t0 = [gettimeofday];
    _with_cdc_disabled(sub {
        for my $i (1..$N) {
            CDCTestSuite::Schema->table('Department')->insert({
                name => "base_$i", location => 'bench',
            });
        }
    });
    my $base_elapsed = tv_interval($t0);
    my $base_rate = $N / ($base_elapsed || 0.001);

    is($CDC->count_events('CDCTestSuite::Schema', table => 'departments'), 0,
        'Baseline: zero CDC events');

    $suite->clean;
    $t0 = [gettimeofday];
    for my $i (1..$N) {
        CDCTestSuite::Schema->table('Department')->insert({
            name => "cdc_$i", location => 'bench',
        });
    }
    my $cdc_elapsed = tv_interval($t0);
    my $cdc_rate = $N / ($cdc_elapsed || 0.001);

    diag sprintf "INSERT benchmark (N=%d): ORM=%.1f ops/s, CDC=%.1f ops/s",
        $N, $base_rate, $cdc_rate;

    ok($base_rate > 0, sprintf "ORM baseline: %.0f ops/s", $base_rate);
    ok($cdc_rate > 0,  sprintf "ORM + CDC:    %.0f ops/s", $cdc_rate);
    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'INSERT'), $N,
        "All $N CDC events captured");
};

subtest 'Performance – UPDATE: ORM vs ORM+CDC' => sub {
    plan tests => 4;
    my $N = $ENV{CDC_PERF_N} || 100;

    $suite->clean;
    _with_cdc_disabled(sub {
        for my $i (1..$N) {
            CDCTestSuite::Schema->table('Department')->insert({
                name => "upd_$i", location => 'before',
            });
        }
    });

    my $rows = CDCTestSuite::Schema->table('Department')
        ->select(-where => { location => 'before' });
    my $t0 = [gettimeofday];
    _with_cdc_disabled(sub {
        for my $row (@$rows) { $row->update({ location => 'mid' }) }
    });
    my $base_elapsed = tv_interval($t0);
    my $base_rate = $N / ($base_elapsed || 0.001);

    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'UPDATE'), 0, 'Baseline: zero');

    $rows = CDCTestSuite::Schema->table('Department')
        ->select(-where => { location => 'mid' });
    $t0 = [gettimeofday];
    for my $row (@$rows) { $row->update({ location => 'after' }) }
    my $cdc_elapsed = tv_interval($t0);
    my $cdc_rate = $N / ($cdc_elapsed || 0.001);

    diag sprintf "UPDATE benchmark (N=%d): ORM=%.1f ops/s, CDC=%.1f ops/s",
        $N, $base_rate, $cdc_rate;

    ok($base_rate > 0, sprintf "ORM baseline: %.0f ops/s", $base_rate);
    ok($cdc_rate > 0,  sprintf "ORM + CDC:    %.0f ops/s", $cdc_rate);
    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'UPDATE'), $N,
        "All $N UPDATE events captured");
};

subtest 'Performance – DELETE: ORM vs ORM+CDC' => sub {
    plan tests => 4;
    my $N = $ENV{CDC_PERF_N} || 100;

    $suite->clean;
    _with_cdc_disabled(sub {
        for my $i (1..$N) {
            CDCTestSuite::Schema->table('Department')->insert({
                name => "dbase_$i", location => 'gone',
            });
        }
    });
    my $rows = CDCTestSuite::Schema->table('Department')
        ->select(-where => { location => 'gone' });
    my $t0 = [gettimeofday];
    _with_cdc_disabled(sub {
        for my $row (@$rows) { $row->delete() }
    });
    my $base_elapsed = tv_interval($t0);
    my $base_rate = $N / ($base_elapsed || 0.001);

    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'DELETE'), 0, 'Baseline: zero');

    $suite->clean;
    _with_cdc_disabled(sub {
        for my $i (1..$N) {
            CDCTestSuite::Schema->table('Department')->insert({
                name => "dcdc_$i", location => 'gone',
            });
        }
    });
    $rows = CDCTestSuite::Schema->table('Department')
        ->select(-where => { location => 'gone' });
    $t0 = [gettimeofday];
    for my $row (@$rows) { $row->delete() }
    my $cdc_elapsed = tv_interval($t0);
    my $cdc_rate = $N / ($cdc_elapsed || 0.001);

    diag sprintf "DELETE benchmark (N=%d): ORM=%.1f ops/s, CDC=%.1f ops/s",
        $N, $base_rate, $cdc_rate;

    ok($base_rate > 0, sprintf "ORM baseline: %.0f ops/s", $base_rate);
    ok($cdc_rate > 0,  sprintf "ORM + CDC:    %.0f ops/s", $cdc_rate);
    is($CDC->count_events('CDCTestSuite::Schema',
        table => 'departments', operation => 'DELETE'), $N,
        "All $N DELETE events captured");
};

# ---------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------
$suite->clean;
$dbh->disconnect();
done_testing();
