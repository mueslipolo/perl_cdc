package DBIx::DataModel::Plugin::CDC::Event;

use strict;
use warnings;
use POSIX qw(strftime);

# ---------------------------------------------------------------
# build(%args) -> \%event
#
# Constructs a canonical CDC event envelope.
#
#   table_name  => 'EMPLOYEES'
#   operation   => 'INSERT' | 'UPDATE' | 'DELETE'
#   schema_name => 'App::Schema'
#   old_data    => \%hash | undef
#   new_data    => \%hash | undef
# ---------------------------------------------------------------
sub build {
    my ($class, %args) = @_;

    my $old = $args{old_data};
    my $new = $args{new_data};

    # Compute changed columns for UPDATE
    my $changed;
    if ($args{operation} eq 'UPDATE' && $old && $new) {
        $changed = [
            sort grep {
                my $o = $old->{$_} // '';
                my $n = $new->{$_} // '';
                $o ne $n;
            } keys %$new
        ];
    }

    return {
        event_id        => _generate_id(),
        occurred_at     => strftime('%Y-%m-%dT%H:%M:%SZ', gmtime),
        schema_name     => $args{schema_name},
        table_name      => $args{table_name},
        operation       => $args{operation},
        old_data        => $old,
        new_data        => $new,
        changed_columns => $changed,
    };
}

# Simple random hex ID — no external UUID dependency.
sub _generate_id {
    my @r = map { int(rand(65536)) } 1 .. 8;
    return sprintf '%04x%04x-%04x-%04x-%04x-%04x%04x%04x', @r;
}

1;
