#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Table;

# Test schema (no DB needed)
DBIx::DataModel->Schema('Test::On::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);
Test::On::Schema->Table(Widget => 'widgets', 'id');

my $CDC = 'DBIx::DataModel::Plugin::CDC';

subtest 'on() – validation' => sub {
    plan tests => 6;
    $CDC->setup('Test::On::Schema', tables => 'all', force => 1);

    throws_ok { $CDC->on() }
        qr/schema class/, 'missing schema croaks';

    throws_ok { $CDC->on('Test::On::Schema') }
        qr/operation/, 'missing operation croaks';

    throws_ok { $CDC->on('Test::On::Schema', 'TYPO', sub {}) }
        qr/invalid operation/, 'invalid operation croaks';

    throws_ok { $CDC->on('Test::On::Schema', 'insert') }
        qr/coderef/, 'missing coderef croaks';

    throws_ok {
        $CDC->on('Test::On::Schema', 'insert', sub {}, { phase => 'bogus' });
    } qr/invalid phase/, 'invalid phase croaks';

    throws_ok {
        $CDC->on('Test::On::Schema', 'insert', sub {}, { on_error => 'explode' });
    } qr/invalid on_error/, 'invalid on_error croaks';
};

subtest 'on() – accepts valid params' => sub {
    plan tests => 3;
    $CDC->setup('Test::On::Schema', tables => 'all', force => 1);

    lives_ok {
        $CDC->on('Test::On::Schema', 'insert', sub { 1 });
    } 'simple callback';

    lives_ok {
        $CDC->on('Test::On::Schema', '*', sub { 1 },
            { phase => 'in_transaction', on_error => 'abort' });
    } 'wildcard with all options';

    lives_ok {
        $CDC->on('Test::On::Schema', 'DELETE', sub { 1 });
    } 'uppercase operation accepted';
};

subtest 'on() before setup() – croaks' => sub {
    plan tests => 1;
    throws_ok {
        $CDC->on('No::Such::Schema', 'insert', sub { 1 });
    } qr/not configured/, 'on() before setup() croaks';
};

subtest 'log_to_dbi – validation' => sub {
    plan tests => 2;
    $CDC->setup('Test::On::Schema', tables => 'all', force => 1);

    lives_ok {
        $CDC->log_to_dbi('Test::On::Schema', 'cdc_events');
    } 'valid table name';

    throws_ok {
        $CDC->log_to_dbi('Test::On::Schema', 'DROP TABLE x;--');
    } qr/Invalid table name/, 'SQL injection rejected';
};

subtest 'log_to_stderr – registers listener' => sub {
    plan tests => 1;
    $CDC->setup('Test::On::Schema', tables => 'all', force => 1);
    lives_ok {
        $CDC->log_to_stderr('Test::On::Schema', 'TEST');
    } 'registers without error';
};

subtest 'chaining – setup -> log_to_dbi -> on' => sub {
    plan tests => 1;
    lives_ok {
        $CDC->setup('Test::On::Schema', tables => 'all', force => 1)
            ->log_to_dbi('Test::On::Schema')
            ->log_to_stderr('Test::On::Schema')
            ->on('Test::On::Schema', '*', sub { 1 });
    } 'full chain works';
};

done_testing();
