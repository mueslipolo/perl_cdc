#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Table;

DBIx::DataModel->Schema('Test::CDC::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);
Test::CDC::Schema->Table(Foo => 'foos', 'id');
Test::CDC::Schema->Table(Bar => 'bars', 'id');

my $CDC = 'DBIx::DataModel::Plugin::CDC';

subtest 'setup – basic configuration' => sub {
    plan tests => 3;

    lives_ok {
        $CDC->setup('Test::CDC::Schema', tables => 'all');
    } 'setup with tables=all succeeds';

    my $cfg = $CDC->config_for('Test::CDC::Schema');
    ok($cfg->{tracked}{Foo}, 'Foo is tracked');
    ok($cfg->{tracked}{Bar}, 'Bar is tracked');
};

subtest 'setup – selective tables' => sub {
    plan tests => 3;

    $CDC->setup('Test::CDC::Schema', tables => ['Foo'], force => 1);

    ok($CDC->is_tracked('Test::CDC::Schema', 'Foo'),  'Foo tracked');
    ok(!$CDC->is_tracked('Test::CDC::Schema', 'Bar'), 'Bar NOT tracked');
    ok(!$CDC->is_tracked('Test::CDC::Schema', 'Baz'), 'Baz NOT tracked');
};

subtest 'setup – validation' => sub {
    plan tests => 1;

    throws_ok {
        $CDC->setup();
    } qr/schema class/, 'missing schema class croaks';
};

subtest 'is_tracked – unconfigured schema' => sub {
    plan tests => 1;
    ok(!$CDC->is_tracked('No::Such::Schema', 'Foo'),
        'unconfigured schema returns false');
};

subtest 'setup() twice – warns about discarded listeners' => sub {
    plan tests => 2;
    $CDC->setup('Test::CDC::Schema', tables => 'all');
    $CDC->on('Test::CDC::Schema', '*' => sub { 1 });

    my $warned = 0;
    local $SIG{__WARN__} = sub { $warned++ if $_[0] =~ /listeners discarded/ };
    $CDC->setup('Test::CDC::Schema', tables => 'all');
    ok($warned, 'warning emitted when listeners would be lost');

    # Config still works after re-setup
    ok($CDC->is_tracked('Test::CDC::Schema', 'Foo'), 'tracking still works');
};

done_testing();
