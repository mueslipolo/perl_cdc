# Runtime dependencies
requires 'DBIx::DataModel',  '>= 3.0';
requires 'DBI',              '>= 1.643';
requires 'Cpanel::JSON::XS', '>= 4.0';
requires 'Params::Validate', '>= 1.30';
requires 'Try::Tiny',        '>= 0.30';
requires 'Scalar::Util';
requires 'Time::HiRes';
requires 'Carp';
requires 'namespace::clean', '>= 0.27';

# Test dependencies
on 'test' => sub {
    requires 'Test::More',      '>= 1.302';
    requires 'Test::Exception';
};

# Integration tests (examples/oracle-cdc-poc)
on 'develop' => sub {
    requires 'DBD::Oracle', '>= 1.83';
};
