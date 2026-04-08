#!/usr/bin/env perl
# =============================================================
# t/10_e2e_sqlite.t
#
# SQLite backend for the shared CDC e2e test suite.
# Runs all common tests + SQLite-specific edge cases.
#
# Requires DBD::SQLite (test dependency only).
# =============================================================

use strict;
use warnings;
use Test::More;

eval { require DBD::SQLite; DBD::SQLite->VERSION(1.50); 1 }
    or plan skip_all => 'DBD::SQLite >= 1.50 required for e2e tests';

use DBI;
use Cpanel::JSON::XS ();
use DBIx::DataModel::Plugin::CDC;

use lib 't/lib';
use CDCTestSuite::Schema;
use CDCTestSuite;

# ---------------------------------------------------------------
# SQLite connection + DDL
# ---------------------------------------------------------------
my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:', '', '', {
    RaiseError       => 1,
    PrintError       => 0,
    AutoCommit       => 1,
    FetchHashKeyName => 'NAME_lc',
});

$dbh->do(q{
    CREATE TABLE departments (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        name     TEXT NOT NULL,
        location TEXT
    )
});
$dbh->do(q{
    CREATE TABLE employees (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        department_id INTEGER REFERENCES departments(id),
        first_name    TEXT NOT NULL,
        last_name     TEXT NOT NULL,
        email         TEXT UNIQUE NOT NULL,
        salary        REAL
    )
});
$dbh->do(q{
    CREATE TABLE cdc_events (
        event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT NOT NULL,
        operation  TEXT NOT NULL,
        old_data   TEXT,
        new_data   TEXT
    )
});

CDCTestSuite::Schema->dbh($dbh);

# ---------------------------------------------------------------
# Run shared test suite
# ---------------------------------------------------------------
my $suite = CDCTestSuite->new(dbh => $dbh);
$suite->setup_cdc(capture_old => 1);
$suite->run_common_suite;

# ===============================================================
# SQLite-specific tests
# ===============================================================

subtest 'SQLite – UTF-8 round-trip' => sub {
    plan tests => 2;
    $suite->clean;

    CDCTestSuite::Schema->table('Department')->insert({
        name => "Gen\x{e8}ve", location => "Z\x{fc}rich",
    });

    my $ev = DBIx::DataModel::Plugin::CDC->latest_event(
        'CDCTestSuite::Schema',
        table => 'departments', operation => 'INSERT');
    my $new = Cpanel::JSON::XS->new->utf8->canonical->allow_nonref
        ->decode($ev->{new_data});
    like($new->{NAME},     qr/Gen.*ve/,  'UTF-8 name round-trips');
    like($new->{LOCATION}, qr/Z.*rich/,  'UTF-8 location round-trips');
};

subtest 'SQLite – empty string is NOT NULL' => sub {
    plan tests => 2;
    $suite->clean;

    CDCTestSuite::Schema->table('Department')->insert({
        name => 'EmptyLoc', location => '',
    });

    my $ev = DBIx::DataModel::Plugin::CDC->latest_event(
        'CDCTestSuite::Schema',
        table => 'departments', operation => 'INSERT');
    my $new = $suite->parse_json($ev->{new_data});
    is($new->{LOCATION}, '', 'empty string preserved');
    ok(defined $new->{LOCATION}, 'defined');
};

done_testing();
