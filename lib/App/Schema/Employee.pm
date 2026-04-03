package App::Schema::Employee;

# =============================================================
# App::Schema::Employee  –  Table class for EMPLOYEES
#
# Extend this class with column inflation/deflation hooks,
# custom methods, or validation logic as the application grows.
#
# Example extension – auto-touch updated_at on every UPDATE:
#
#   use POSIX qw(strftime);
#
#   around 'update' => sub {
#       my ($orig, $self, $data, @rest) = @_;
#       $data->{updated_at} = strftime('%Y-%m-%d %H:%M:%S', localtime);
#       $self->$orig($data, @rest);
#   };
# =============================================================

use strict;
use warnings;

use parent -norequire, 'App::Schema';

1;

__END__

=head1 NAME

App::Schema::Employee – DBIx::DataModel table class for EMPLOYEES

=head1 DESCRIPTION

Corresponds to the C<employees> Oracle table.

Columns: C<id>, C<department_id>, C<first_name>, C<last_name>,
C<email>, C<salary>, C<active>, C<created_at>, C<updated_at>.

=cut
