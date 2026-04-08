#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Table;
use DBIx::DataModel::Plugin::CDC::Event;

my $CDC = 'DBIx::DataModel::Plugin::CDC';

# Test schema
DBIx::DataModel->Schema('Test::Dispatch::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);
Test::Dispatch::Schema->Table(Item => 'items', 'id');

my $event = DBIx::DataModel::Plugin::CDC::Event->build(
    schema_name => 'Test::Dispatch::Schema',
    table_name  => 'ITEMS',
    operation   => 'INSERT',
    old_data    => undef,
    new_data    => { ID => 1 },
);

# Mock schema object (no real DB)
my $mock_schema = bless {}, 'Test::Dispatch::Schema';

subtest 'dispatch – operation filtering' => sub {
    plan tests => 2;
    $CDC->setup('Test::Dispatch::Schema', tables => 'all', force => 1);

    my @seen;
    $CDC->on('Test::Dispatch::Schema', 'INSERT', sub { push @seen, 'ins' },
        { phase => 'in_transaction' });
    $CDC->on('Test::Dispatch::Schema', 'UPDATE', sub { push @seen, 'upd' },
        { phase => 'in_transaction' });

    $CDC->dispatch('Test::Dispatch::Schema', $mock_schema, $event);
    is(scalar @seen, 1, 'Only INSERT listener fired');
    is($seen[0], 'ins', 'Correct listener');
};

subtest 'dispatch – wildcard matches all' => sub {
    plan tests => 1;
    $CDC->setup('Test::Dispatch::Schema', tables => 'all', force => 1);

    my $called = 0;
    $CDC->on('Test::Dispatch::Schema', '*', sub { $called++ },
        { phase => 'in_transaction' });

    $CDC->dispatch('Test::Dispatch::Schema', $mock_schema, $event);
    ok($called, 'Wildcard listener fired');
};

subtest 'dispatch – multiple listeners in order' => sub {
    plan tests => 1;
    $CDC->setup('Test::Dispatch::Schema', tables => 'all', force => 1);

    my @order;
    $CDC->on('Test::Dispatch::Schema', '*', sub { push @order, 'first' },
        { phase => 'in_transaction' });
    $CDC->on('Test::Dispatch::Schema', '*', sub { push @order, 'second' },
        { phase => 'in_transaction' });

    $CDC->dispatch('Test::Dispatch::Schema', $mock_schema, $event);
    is_deeply(\@order, ['first', 'second'], 'Registration order preserved');
};

subtest 'error policy: warn' => sub {
    plan tests => 2;
    $CDC->setup('Test::Dispatch::Schema', tables => 'all', force => 1);

    $CDC->on('Test::Dispatch::Schema', '*', sub { die "boom" },
        { phase => 'in_transaction', on_error => 'warn' });

    my $warned = 0;
    local $SIG{__WARN__} = sub { $warned++ if $_[0] =~ /boom/ };
    lives_ok { $CDC->dispatch('Test::Dispatch::Schema', $mock_schema, $event) }
        'warn policy does not die';
    ok($warned, 'warning emitted');
};

subtest 'error policy: abort' => sub {
    plan tests => 1;
    $CDC->setup('Test::Dispatch::Schema', tables => 'all', force => 1);

    $CDC->on('Test::Dispatch::Schema', '*', sub { die "critical" },
        { phase => 'in_transaction', on_error => 'abort' });

    throws_ok { $CDC->dispatch('Test::Dispatch::Schema', $mock_schema, $event) }
        qr/critical/, 'abort policy propagates';
};

subtest 'error policy: ignore' => sub {
    plan tests => 2;
    $CDC->setup('Test::Dispatch::Schema', tables => 'all', force => 1);

    $CDC->on('Test::Dispatch::Schema', '*', sub { die "silent" },
        { phase => 'in_transaction', on_error => 'ignore' });

    my $warned = 0;
    local $SIG{__WARN__} = sub { $warned++ if $_[0] =~ /ignored/ };
    lives_ok { $CDC->dispatch('Test::Dispatch::Schema', $mock_schema, $event) }
        'ignore policy does not die';
    ok($warned, 'ignore policy still warns');
};

subtest 'abort does not prevent subsequent listeners from cleanup' => sub {
    plan tests => 2;
    $CDC->setup('Test::Dispatch::Schema', tables => 'all', force => 1);

    my $first_ran = 0;
    $CDC->on('Test::Dispatch::Schema', '*', sub { $first_ran = 1 },
        { phase => 'in_transaction' });
    $CDC->on('Test::Dispatch::Schema', '*', sub { die "abort" },
        { phase => 'in_transaction', on_error => 'abort' });

    throws_ok { $CDC->dispatch('Test::Dispatch::Schema', $mock_schema, $event) }
        qr/abort/, 'abort propagated';
    ok($first_ran, 'First listener ran before abort');
};

done_testing();
