#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::DataModel::Plugin::CDC::Handler;

subtest 'abstract methods enforced' => sub {
    plan tests => 2;
    my $h = DBIx::DataModel::Plugin::CDC::Handler->new();
    throws_ok { $h->dispatch_event({}, undef) }
        qr/should implement.*dispatch_event/i, 'dispatch_event is abstract';
    throws_ok { $h->phase() }
        qr/should implement.*phase/i, 'phase is abstract';
};

subtest 'DBI handler – constructor validation' => sub {
    plan tests => 2;
    use DBIx::DataModel::Plugin::CDC::Handler::DBI;

    lives_ok {
        DBIx::DataModel::Plugin::CDC::Handler::DBI->new(table_name => 'cdc_events');
    } 'valid table_name accepted';

    throws_ok {
        DBIx::DataModel::Plugin::CDC::Handler::DBI->new(table_name => 'DROP TABLE x; --');
    } qr/did not pass/, 'SQL injection in table_name rejected';
};

subtest 'Callback handler – constructor validation' => sub {
    plan tests => 3;
    use DBIx::DataModel::Plugin::CDC::Handler::Callback;

    lives_ok {
        DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
            on_event => sub { 1 }, phase => 'post_commit', on_error => 'warn',
        );
    } 'valid params accepted';

    throws_ok {
        DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
            on_event => 'not a coderef',
        );
    } qr/type/, 'non-coderef on_event rejected';

    throws_ok {
        DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
            on_event => sub { 1 }, phase => 'invalid',
        );
    } qr/did not pass/, 'invalid phase rejected';
};

subtest 'Log handler – defaults' => sub {
    plan tests => 2;
    use DBIx::DataModel::Plugin::CDC::Handler::Log;

    my $h = DBIx::DataModel::Plugin::CDC::Handler::Log->new();
    is($h->phase, 'post_commit', 'default phase');

    my $h2 = DBIx::DataModel::Plugin::CDC::Handler::Log->new(prefix => 'AUDIT');
    my $output = '';
    local $SIG{__WARN__} = sub { $output .= $_[0] };
    $h2->dispatch_event({
        table_name => 'USERS', operation => 'INSERT', event_id => 'test-id',
    }, undef);
    like($output, qr/\[AUDIT\] USERS INSERT test-id/, 'custom prefix works');
};

subtest 'Multi handler – constructor validation' => sub {
    plan tests => 2;
    use DBIx::DataModel::Plugin::CDC::Handler::Multi;

    throws_ok {
        DBIx::DataModel::Plugin::CDC::Handler::Multi->new(handlers => []);
    } qr/at least one/, 'empty handlers rejected';

    my $cb = DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
        on_event => sub { 1 }, phase => 'in_transaction',
    );
    lives_ok {
        DBIx::DataModel::Plugin::CDC::Handler::Multi->new(handlers => [$cb]);
    } 'valid handlers accepted';
};

done_testing();
