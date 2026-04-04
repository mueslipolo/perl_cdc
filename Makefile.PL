use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'DBIx::DataModel::Plugin::CDC',
    VERSION_FROM     => 'lib/DBIx/DataModel/Plugin/CDC.pm',
    ABSTRACT_FROM    => 'lib/DBIx/DataModel/Plugin/CDC.pm',
    AUTHOR           => 'Yves',
    LICENSE          => 'apache_2_0',
    MIN_PERL_VERSION => '5.020',

    PREREQ_PM => {
        'DBIx::DataModel'  => '3.0',
        'DBI'              => '1.643',
        'Cpanel::JSON::XS' => '4.0',
        'Try::Tiny'        => '0.30',
        'Scalar::Util'     => '0',
        'Time::HiRes'      => '0',
        'Carp'             => '0',
        'namespace::clean' => '0.27',
    },

    TEST_REQUIRES => {
        'Test::More'      => '1.302',
        'Test::Exception' => '0',
    },

    META_MERGE => {
        'meta-spec' => { version => 2 },
        resources   => {
            repository => {
                type => 'git',
                url  => 'https://github.com/mueslipolo/perl_cdc.git',
                web  => 'https://github.com/mueslipolo/perl_cdc',
            },
            bugtracker => {
                web => 'https://github.com/mueslipolo/perl_cdc/issues',
            },
        },
        no_index => {
            directory => ['examples'],
        },
    },

    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz' },
    clean => { FILES    => 'DBIx-DataModel-Plugin-CDC-*' },
);
