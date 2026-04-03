package DBIx::DataModel::Plugin::CDC::Handler::Callback;

use strict;
use warnings;
use parent 'DBIx::DataModel::Plugin::CDC::Handler';

use Carp qw(croak);
use Params::Validate qw(validate_with CODEREF SCALAR);
use namespace::clean;

our $VERSION = '1.01';

my $new_spec = {
    on_event => { type => CODEREF },
    phase    => { type => SCALAR, default => 'post_commit',
                  regex => qr/\A(?:in_transaction|post_commit)\z/ },
    on_error => { type => SCALAR, default => 'warn',
                  regex => qr/\A(?:abort|warn|ignore)\z/ },
};

sub new {
    my $class = shift;
    my %args = validate_with(params => \@_, spec => $new_spec);
    return bless \%args, $class;
}

sub phase    { $_[0]->{phase} }
sub on_error { $_[0]->{on_error} }

sub dispatch_event {
    my ($self, $event, $schema) = @_;
    $self->{on_event}->($event, $schema);
}

1;
