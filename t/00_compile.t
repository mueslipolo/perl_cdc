#!/usr/bin/env perl
use strict;
use warnings;
use Test::More tests => 8;

use_ok('DBIx::DataModel::Plugin::CDC');
use_ok('DBIx::DataModel::Plugin::CDC::Table');
use_ok('DBIx::DataModel::Plugin::CDC::Event');
use_ok('DBIx::DataModel::Plugin::CDC::Handler');
use_ok('DBIx::DataModel::Plugin::CDC::Handler::DBI');
use_ok('DBIx::DataModel::Plugin::CDC::Handler::Callback');
use_ok('DBIx::DataModel::Plugin::CDC::Handler::Log');
use_ok('DBIx::DataModel::Plugin::CDC::Handler::Multi');
