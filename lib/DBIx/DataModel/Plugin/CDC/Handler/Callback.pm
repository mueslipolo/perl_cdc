package DBIx::DataModel::Plugin::CDC::Handler::Callback;

use strict;
use warnings;
use Carp qw(croak);

sub new {
    my ($class, %args) = @_;
    croak 'on_event coderef required' unless ref $args{on_event} eq 'CODE';
    return bless {
        on_event => $args{on_event},
        phase    => $args{phase}    // 'post_commit',
        on_error => $args{on_error} // 'warn',
    }, $class;
}

sub phase { $_[0]->{phase} }

sub on_error { $_[0]->{on_error} }

sub dispatch_event {
    my ($self, $event, $schema) = @_;
    $self->{on_event}->($event, $schema);
}

1;
