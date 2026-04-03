#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Table;
use DBIx::DataModel::Plugin::CDC::Handler::Callback;

# Create a test schema (no DB connection needed for setup tests)
DBIx::DataModel->Schema('Test::CDC::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);
Test::CDC::Schema->Table(Foo => 'foos', 'id');
Test::CDC::Schema->Table(Bar => 'bars', 'id');

subtest 'setup – basic configuration' => sub {
    plan tests => 4;

    my $cb = DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
        on_event => sub { 1 },
    );

    lives_ok {
        DBIx::DataModel::Plugin::CDC->setup('Test::CDC::Schema',
            tables   => 'all',
            handlers => [$cb],
        );
    } 'setup with tables=all succeeds';

    my $cfg = DBIx::DataModel::Plugin::CDC->config_for('Test::CDC::Schema');
    ok($cfg, 'config stored');
    ok($cfg->{tracked}{Foo}, 'Foo is tracked');
    ok($cfg->{tracked}{Bar}, 'Bar is tracked');
};

subtest 'setup – selective tables' => sub {
    plan tests => 3;

    my $cb = DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
        on_event => sub { 1 },
    );

    DBIx::DataModel::Plugin::CDC->setup('Test::CDC::Schema',
        tables   => ['Foo'],
        handlers => [$cb],
    );

    ok(DBIx::DataModel::Plugin::CDC->is_tracked('Test::CDC::Schema', 'Foo'),
        'Foo is tracked');
    ok(!DBIx::DataModel::Plugin::CDC->is_tracked('Test::CDC::Schema', 'Bar'),
        'Bar is NOT tracked');
    ok(!DBIx::DataModel::Plugin::CDC->is_tracked('Test::CDC::Schema', 'Baz'),
        'nonexistent table is not tracked');
};

subtest 'setup – validation' => sub {
    plan tests => 3;

    throws_ok {
        DBIx::DataModel::Plugin::CDC->setup();
    } qr/schema class/, 'missing schema class croaks';

    throws_ok {
        DBIx::DataModel::Plugin::CDC->setup('Test::CDC::Schema');
    } qr/handlers/, 'missing handlers croaks';

    throws_ok {
        DBIx::DataModel::Plugin::CDC->setup('Test::CDC::Schema',
            handlers => [], tables => 'all');
    } qr/handlers/, 'empty handlers croaks';
};

subtest 'is_tracked – unconfigured schema' => sub {
    plan tests => 1;
    ok(!DBIx::DataModel::Plugin::CDC->is_tracked('No::Such::Schema', 'Foo'),
        'unconfigured schema returns false');
};

done_testing();
