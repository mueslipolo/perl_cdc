package App::Schema;

# =============================================================
# App::Schema  –  DBIx::DataModel schema declaration
#
# All table classes and associations for the CDC PoC application
# are declared here.  The schema is used by the end-to-end test
# and can be reused by any application code that imports it.
# =============================================================

use strict;
use warnings;

use DBIx::DataModel;

DBIx::DataModel->Schema('App::Schema');

# ---------------------------------------------------------------
# Table declarations
#   Schema->Table( ClassName => 'sql_table_name', @primary_key )
# ---------------------------------------------------------------
App::Schema->Table( Department => 'departments', 'id' );
App::Schema->Table( Employee   => 'employees',   'id' );

# ---------------------------------------------------------------
# Associations
#   One department has many employees (standard 1-to-*)
# ---------------------------------------------------------------
App::Schema->Association(
    [ Department => 'department', '1' ],
    [ Employee   => 'employees',  '*' ],
);

# ---------------------------------------------------------------
# CDC hook registration
#
# Call install_cdc($dbh) after connecting to install ORM-level
# change capture on all tracked tables.
# ---------------------------------------------------------------
sub install_cdc {
    my ($class, $dbh) = @_;
    require CDC::Manager;
    my $cdc = CDC::Manager->new(dbh => $dbh);
    $cdc->track_table('App::Schema::Department', 'departments');
    $cdc->track_table('App::Schema::Employee',   'employees');
    return $cdc;
}

1;

__END__

=head1 NAME

App::Schema – DBIx::DataModel ORM schema for the CDC PoC

=head1 SYNOPSIS

    use App::Schema;

    App::Schema->dbh($dbh);
    my $cdc = App::Schema->install_cdc($dbh);

    my $dept = App::Schema->table('Department')->insert({
        name     => 'Engineering',
        location => 'Geneva',
    });

=cut
