package DBIx::DataModel::Plugin::CDC::Handler::Log;

use strict;
use warnings;
use parent 'DBIx::DataModel::Plugin::CDC::Handler';

use Params::Validate qw(validate_with SCALAR);
use namespace::clean;

our $VERSION = '1.01';

my $new_spec = {
    prefix => { type => SCALAR, default => 'CDC' },
};

sub new {
    my $class = shift;
    my %args = validate_with(params => \@_, spec => $new_spec);
    return bless \%args, $class;
}

sub phase { 'post_commit' }

sub dispatch_event {
    my ($self, $event, $schema) = @_;
    warn sprintf "[%s] %s %s %s\n",
        $self->{prefix},
        $event->{table_name},
        $event->{operation},
        $event->{event_id};
}

1;
