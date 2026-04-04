#!/usr/bin/env perl
use strict;
use warnings;
use Test::More tests => 3;

use_ok('DBIx::DataModel::Plugin::CDC');
use_ok('DBIx::DataModel::Plugin::CDC::Table');
use_ok('DBIx::DataModel::Plugin::CDC::Event');
