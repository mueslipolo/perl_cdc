#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::DataModel::Plugin::CDC::Handler::Callback;
use DBIx::DataModel::Plugin::CDC::Handler::Multi;

my $event = {
    event_id   => 'test',
    table_name => 'T',
    operation  => 'INSERT',
    old_data   => undef,
    new_data   => { ID => 1 },
};

subtest 'dispatches to in_transaction handlers' => sub {
    plan tests => 2;
    my @seen;
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { push @seen, 'h1' },
                phase    => 'in_transaction',
            ),
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { push @seen, 'h2' },
                phase    => 'in_transaction',
            ),
        ],
    );
    $multi->dispatch_event($event, undef);
    is(scalar @seen, 2, 'both handlers called');
    is_deeply(\@seen, ['h1', 'h2'], 'in registration order');
};

subtest 'separates in_transaction and post_commit' => sub {
    plan tests => 2;
    my @in_txn;
    my @post;
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { push @in_txn, 1 },
                phase    => 'in_transaction',
            ),
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { push @post, 1 },
                phase    => 'post_commit',
            ),
        ],
    );

    $multi->dispatch_event($event, undef);
    is(scalar @in_txn, 1, 'in_transaction handler called');

    $multi->dispatch_post_commit($event, undef);
    is(scalar @post, 1, 'post_commit handler called');
};

subtest 'has_post_commit_handlers' => sub {
    plan tests => 2;
    my $no_post = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { 1 }, phase => 'in_transaction',
            ),
        ],
    );
    ok(!$no_post->has_post_commit_handlers, 'no post_commit handlers');

    my $with_post = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { 1 }, phase => 'post_commit',
            ),
        ],
    );
    ok($with_post->has_post_commit_handlers, 'has post_commit handlers');
};

subtest 'error policy: warn' => sub {
    plan tests => 2;
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { die "boom" },
                phase    => 'in_transaction',
                on_error => 'warn',
            ),
        ],
    );
    my $warned = 0;
    local $SIG{__WARN__} = sub { $warned++ if $_[0] =~ /boom/ };
    lives_ok { $multi->dispatch_event($event, undef) } 'warn policy does not die';
    ok($warned, 'warning was emitted');
};

subtest 'error policy: abort' => sub {
    plan tests => 1;
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { die "critical" },
                phase    => 'in_transaction',
                on_error => 'abort',
            ),
        ],
    );
    throws_ok { $multi->dispatch_event($event, undef) }
        qr/critical/, 'abort policy propagates exception';
};

subtest 'error policy: ignore' => sub {
    plan tests => 1;
    my $multi = DBIx::DataModel::Plugin::CDC::Handler::Multi->new(
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { die "silent" },
                phase    => 'in_transaction',
                on_error => 'ignore',
            ),
        ],
    );
    lives_ok { $multi->dispatch_event($event, undef) } 'ignore policy swallows';
};

done_testing();
