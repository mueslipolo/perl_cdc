package DBIx::DataModel::Plugin::CDC::Handler::Multi;

use strict;
use warnings;
use Carp qw(croak);
use Try::Tiny;

sub new {
    my ($class, %args) = @_;
    croak 'handlers arrayref required'
        unless ref $args{handlers} eq 'ARRAY' && @{$args{handlers}};
    return bless {
        handlers => $args{handlers},
        on_error => $args{on_error} // 'warn',
    }, $class;
}

# Multi dispatches in both phases — it delegates phase logic internally.
sub phase { 'in_transaction' }

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
            }
            # 'ignore' — silently swallow
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
    return grep { $_->phase eq 'post_commit' } @{ $self->{handlers} };
}

1;
