#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::DataModel::Plugin::CDC::Event;

subtest 'build – INSERT event' => sub {
    plan tests => 7;
    my $ev = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'My::Schema',
        table_name  => 'USERS',
        operation   => 'INSERT',
        old_data    => undef,
        new_data    => { ID => 1, NAME => 'Alice' },
    );
    ok(defined $ev->{cdc_event_id},    'cdc_event_id generated');
    ok(defined $ev->{occurred_at}, 'occurred_at generated');
    like($ev->{occurred_at}, qr/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/,
        'occurred_at is ISO 8601');
    is($ev->{schema_name}, 'My::Schema', 'schema_name');
    is($ev->{table_name},  'USERS',      'table_name');
    is($ev->{operation},   'INSERT',     'operation');
    ok(!defined $ev->{changed_columns}, 'no changed_columns for INSERT');
};

subtest 'build – UPDATE with changed_columns' => sub {
    plan tests => 3;
    my $ev = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X',
        table_name  => 'T',
        operation   => 'UPDATE',
        old_data    => { A => 1, B => 'old', C => 'same' },
        new_data    => { A => 1, B => 'new', C => 'same' },
    );
    is_deeply($ev->{changed_columns}, ['B'], 'only B changed');
    ok(!grep({ $_ eq 'A' } @{$ev->{changed_columns}}), 'A not in changed');

    # Key in old but absent from new should be detected
    my $ev2 = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X',
        table_name  => 'T',
        operation   => 'UPDATE',
        old_data    => { A => 1, B => 2, REMOVED => 'gone' },
        new_data    => { A => 1, B => 2 },
    );
    is_deeply($ev2->{changed_columns}, ['REMOVED'],
        'column in old but not in new is detected');
};

subtest 'build – DELETE event' => sub {
    plan tests => 2;
    my $ev = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'X',
        table_name  => 'T',
        operation   => 'DELETE',
        old_data    => { ID => 42 },
        new_data    => undef,
    );
    ok(!defined $ev->{changed_columns}, 'no changed_columns for DELETE');
    ok(!defined $ev->{new_data}, 'new_data undef');
};

subtest 'build – unique IDs' => sub {
    plan tests => 1;
    my %ids;
    for (1..1000) {
        my $ev = DBIx::DataModel::Plugin::CDC::Event->build(
            schema_name => 'X', table_name => 'T', operation => 'INSERT',
            old_data => undef, new_data => { I => $_ },
        );
        $ids{ $ev->{cdc_event_id} }++;
    }
    is(scalar keys %ids, 1000, '1000 unique IDs generated');
};

subtest 'build – required args validation' => sub {
    plan tests => 2;

    throws_ok {
        DBIx::DataModel::Plugin::CDC::Event->build(
            table_name => 'T', old_data => undef, new_data => { A => 1 },
        );
    } qr/requires operation/, 'missing operation croaks';

    throws_ok {
        DBIx::DataModel::Plugin::CDC::Event->build(
            operation => 'INSERT', old_data => undef, new_data => { A => 1 },
        );
    } qr/requires table_name/, 'missing table_name croaks';
};

done_testing();
