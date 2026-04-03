package DBIx::DataModel::Plugin::CDC::Handler::Log;

use strict;
use warnings;

sub new {
    my ($class, %args) = @_;
    return bless {
        prefix => $args{prefix} // 'CDC',
    }, $class;
}

sub phase { 'post_commit' }

sub dispatch_event {
    my ($self, $event, $schema) = @_;
    my $pfx = $self->{prefix};
    warn sprintf "[%s] %s %s %s\n",
        $pfx,
        $event->{table_name},
        $event->{operation},
        $event->{event_id};
}

1;
