package DBIx::DataModel::Plugin::CDC::Event;

use strict;
use warnings;
use Time::HiRes ();

our $VERSION = '2.00';

my $_pid_hex = sprintf '%04x', $$ & 0xFFFF;
my $_counter = 0;

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
        event_id        => _generate_id($sec, $usec),
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
    $_counter = ($_counter + 1) & 0xFFFF;
    return sprintf '%08x-%04x-%s-%04x', $sec, $usec >> 4, $_pid_hex, $_counter;
}

{
    my $_last_sec = 0;
    my $_last_ts  = '';

    sub _format_ts {
        my ($sec) = @_;
        if ($sec != $_last_sec) {
            my @t = gmtime($sec);
            $_last_ts = sprintf '%04d-%02d-%02dT%02d:%02d:%02dZ',
                $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0];
            $_last_sec = $sec;
        }
        return $_last_ts;
    }
}

1;
