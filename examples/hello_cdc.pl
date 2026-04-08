#!/usr/bin/env perl
# =============================================================
# hello_cdc.pl — Minimal smoke test for DBIx::DataModel::Plugin::CDC
#
# Usage:
#   ./dev.sh hello            # easiest
#   perl -Ilib -Ilocal/lib/perl5 examples/hello_cdc.pl
#
# Requires: DBD::SQLite (uses in-memory database, no setup needed)
# =============================================================

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib", "$FindBin::Bin/../local/lib/perl5";
use DBI;
use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Table;
use Cpanel::JSON::XS ();

my $JSON = Cpanel::JSON::XS->new->utf8->canonical->pretty;

# ---------------------------------------------------------------
# 1. SQLite in-memory database
# ---------------------------------------------------------------
my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:', '', '', {
    RaiseError       => 1,
    AutoCommit       => 1,
    FetchHashKeyName => 'NAME_lc',
});

$dbh->do(q{
    CREATE TABLE users (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        name  TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL
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

# ---------------------------------------------------------------
# 2. Schema declaration with CDC table_parent
# ---------------------------------------------------------------
DBIx::DataModel->Schema('Hello::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);
Hello::Schema->Table(User => 'users', 'id');
Hello::Schema->dbh($dbh);

# ---------------------------------------------------------------
# 3. Configure CDC: log to DB + print to console
# ---------------------------------------------------------------
DBIx::DataModel::Plugin::CDC
    ->setup('Hello::Schema', tables => 'all', capture_old => 1)
    ->log_to_dbi('Hello::Schema', 'cdc_events')
    ->on('Hello::Schema', '*' => sub {
        my ($event) = @_;
        printf "  [CDC] %-6s %s  row_id=%s\n",
            $event->{operation},
            $event->{table_name},
            join(',', map { "$_=$event->{row_id}{$_}" }
                sort keys %{ $event->{row_id} || {} });
    });

# ---------------------------------------------------------------
# 4. Do some DML — events are captured automatically
# ---------------------------------------------------------------
print "--- INSERT ---\n";
Hello::Schema->table('User')->insert({
    name => 'Alice', email => 'alice@example.com',
});

print "\n--- UPDATE ---\n";
my $user = Hello::Schema->table('User')
    ->select(-where => { email => 'alice@example.com' })->[0];
$user->update({ name => 'Alice Dupont' });

print "\n--- DELETE ---\n";
$user->delete;

# ---------------------------------------------------------------
# 5. Query captured events back from the DB
# ---------------------------------------------------------------
print "\n--- Captured events in cdc_events table ---\n\n";

my $events = DBIx::DataModel::Plugin::CDC->events_for(
    'Hello::Schema', table => 'users');

for my $ev (@$events) {
    printf "event_id=%-3d  op=%-6s  new_data=%s\n",
        $ev->{event_id},
        $ev->{operation},
        $ev->{new_data} // 'NULL';
}

printf "\nTotal: %d events captured.\n", scalar @$events;
