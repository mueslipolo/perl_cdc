package App::Schema;

use strict;
use warnings;

use DBIx::DataModel;
use DBIx::DataModel::Plugin::CDC::Table;

DBIx::DataModel->Schema('App::Schema',
    table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
);

App::Schema->Table( Department => 'departments', 'id' );
App::Schema->Table( Employee   => 'employees',   'id' );

App::Schema->Association(
    [ Department => 'department', '1' ],
    [ Employee   => 'employees',  '*' ],
);

1;
