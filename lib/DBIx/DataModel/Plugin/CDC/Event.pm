package DBIx::DataModel::Plugin::CDC::Event;

use strict;
use warnings;
use Carp qw(croak);
use Time::HiRes ();
use namespace::clean;

our $VERSION = '1.01';

my $_pid_hex = sprintf '%04x', $$ & 0xFFFF;
my $_counter = 0;

sub build {
    my ($class, %args) = @_;

    croak 'Event::build requires table_name' unless defined $args{table_name};
    croak 'Event::build requires operation'  unless defined $args{operation};
    croak "Event::build: invalid operation '$args{operation}'"
        unless $args{operation} =~ /\A(?:INSERT|UPDATE|DELETE)\z/;

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

    my ($sec, $usec) = Time::HiRes::gettimeofday();

    return {
        event_id        => _generate_id($sec, $usec),
        occurred_at     => _format_ts($sec),
        schema_name     => $args{schema_name},
        table_name      => $args{table_name},
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
