package DBIx::DataModel::Plugin::CDC::Handler::Multi;

use strict;
use warnings;
use parent 'DBIx::DataModel::Plugin::CDC::Handler';

use Carp qw(croak);
use Params::Validate qw(validate_with ARRAYREF SCALAR);
use Try::Tiny;
use namespace::clean;

our $VERSION = '1.01';

my $new_spec = {
    handlers => { type => ARRAYREF,
                  callbacks => { 'at least one handler' =>
                      sub { ref $_[0] eq 'ARRAY' && @{$_[0]} } } },
    on_error => { type => SCALAR, default => 'warn',
                  regex => qr/\A(?:abort|warn|ignore)\z/ },
};

sub new {
    my $class = shift;
    my %args = validate_with(params => \@_, spec => $new_spec);
    return bless \%args, $class;
}

# Multi dispatches in both phases — delegates phase logic internally.
sub phase { 'in_transaction' }

sub on_error { $_[0]->{on_error} }

sub dispatch_event_for_phase {
    my ($self, $phase, $event, $schema) = @_;

    for my $h (@{ $self->{handlers} }) {
        next unless $h->phase eq $phase;

        my $error_policy = $h->can('on_error')
            ? $h->on_error
            : $self->{on_error};

        try {
            $h->dispatch_event($event, $schema);
        } catch {
            my $err = $_;
            if ($error_policy eq 'abort') {
                die $err;
            } elsif ($error_policy eq 'warn') {
                warn "CDC handler " . ref($h) . " failed: $err";
            } else {
                # 'ignore' — log at debug level only
                warn "CDC handler " . ref($h) . " failed (ignored): $err"
                    if $ENV{CDC_DEBUG};
            }
        };
    }
}

sub dispatch_event {
    my ($self, $event, $schema) = @_;
    $self->dispatch_event_for_phase('in_transaction', $event, $schema);
}

sub dispatch_post_commit {
    my ($self, $event, $schema) = @_;
    $self->dispatch_event_for_phase('post_commit', $event, $schema);
}

sub has_post_commit_handlers {
    my ($self) = @_;
    return !! grep { $_->phase eq 'post_commit' } @{ $self->{handlers} };
}

1;
