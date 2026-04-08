package DBIx::DataModel::Plugin::CDC::Event;

use strict;
use warnings;
use feature 'state';
use Time::HiRes ();

our $VERSION = '2.00';

my $_process_id = $$;
my $_pid_hex    = sprintf '%04x', $_process_id % 65536;
my $_counter    = 0;

sub build {
    my ($class, %args) = @_;

    my $old = $args{old_data};
    my $new = $args{new_data};

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

    my ($sec, $usec) = Time::HiRes::gettimeofday();

    return {
        cdc_event_id    => _generate_id($sec, $usec),
        occurred_at     => _format_ts($sec),
        schema_name     => $args{schema_name},
        table_name      => $args{table_name},
        primary_key     => $args{primary_key},
        row_id          => $args{row_id},
        operation       => $args{operation},
        old_data        => $old,
        new_data        => $new,
        changed_columns => $changed,
    };
}

sub _generate_id {
    my ($sec, $usec) = @_;
    $_counter = ($_counter + 1) % 65536;
    return sprintf '%08x-%04x-%s-%04x', $sec, $usec >> 4, $_pid_hex, $_counter;
}

sub _format_ts {
    my ($sec) = @_;
    state $last_sec = 0;
    state $last_ts  = '';

    if ($sec != $last_sec) {
        my @t = gmtime($sec);
        $last_ts = sprintf '%04d-%02d-%02dT%02d:%02d:%02dZ',
            $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0];
        $last_sec = $sec;
    }
    return $last_ts;
}

1;

__END__

=head1 NAME

DBIx::DataModel::Plugin::CDC::Event - CDC event envelope builder

=head1 DESCRIPTION

Factory class that builds event envelope hashrefs.

=head2 build(%args)

    my $event = DBIx::DataModel::Plugin::CDC::Event->build(
        schema_name => 'App::Schema',
        table_name  => 'EMPLOYEES',
        operation   => 'UPDATE',          # INSERT | UPDATE | DELETE
        primary_key => ['ID'],            # optional, auto-populated by Table
        row_id      => { ID => 42 },      # optional
        old_data    => \%before,          # undef for INSERT or capture_old=0
        new_data    => \%after,           # undef for DELETE
    );

Returns a hashref with the following fields:

    cdc_event_id    Hex string: seconds-usec-pid-counter (unique per process)
    occurred_at     ISO 8601 UTC timestamp (second precision, cached)
    schema_name     From args
    table_name      From args
    primary_key     From args — PK column names as arrayref
    row_id          From args — PK values as hashref
    operation       From args
    old_data        From args
    new_data        From args
    changed_columns Arrayref of column names that differ between old and new
                    (only for UPDATE when both old_data and new_data are present)

=head2 Event ID Format

    SSSSSSSS-UUUU-PPPP-CCCC

    S = seconds since epoch (hex, 8 digits)
    U = microseconds >> 4 (hex, 4 digits)
    P = process ID mod 65536 (hex, 4 digits)
    C = per-process counter mod 65536 (hex, 4 digits)

IDs are monotonically increasing within a single process.  For global
ordering across processes, use the database-generated C<event_id> column
from C<log_to_dbi>.

=cut
