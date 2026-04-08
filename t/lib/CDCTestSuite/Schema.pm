package CDCTestSuite::Schema;

use strict;
use warnings;

use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC::Table;

DBIx::DataModel->Schema('CDCTestSuite::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);

CDCTestSuite::Schema->Table(Department => 'departments', 'id');
CDCTestSuite::Schema->Table(Employee   => 'employees',   'id');

CDCTestSuite::Schema->Composition(
    [Department => 'department', '1', 'id'],
    [Employee   => 'employees',  '*', 'department_id'],
);

1;

__END__

=head1 NAME

CDCTestSuite::Schema - Shared test schema for CDC e2e tests

=head1 DESCRIPTION

Declares a DBIx::DataModel schema with Department and Employee tables
linked by a Composition.  Used by both the SQLite and Oracle test
backends.  Contains no database-specific logic.

=cut
