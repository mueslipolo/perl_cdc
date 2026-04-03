package App::Schema::Department;

# =============================================================
# App::Schema::Department  –  Table class for DEPARTMENTS
#
# Extend this class with column inflation/deflation hooks,
# custom methods, or validation logic as the application grows.
# =============================================================

use strict;
use warnings;

# DBIx::DataModel creates this package automatically via the
# Schema->Table(...) declaration in App::Schema.  We load the
# parent schema here so that the class can be used standalone.
use parent -norequire, 'App::Schema';

1;

__END__

=head1 NAME

App::Schema::Department – DBIx::DataModel table class for DEPARTMENTS

=head1 DESCRIPTION

Corresponds to the C<departments> table.

Columns: C<id>, C<name>, C<location>, C<created_at>.

=cut
