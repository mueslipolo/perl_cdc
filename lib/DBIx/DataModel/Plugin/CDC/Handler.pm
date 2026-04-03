package DBIx::DataModel::Plugin::CDC::Handler;

# =============================================================
# Abstract base class for CDC event handlers.
#
# Subclasses MUST implement:
#   dispatch_event($event, $schema)
#   phase()  — returns 'in_transaction' or 'post_commit'
#
# Follows DBIx::DataModel conventions:
#   - Raw bless, no Moo/Moose
#   - define_abstract_methods for interface contract
#   - croak for errors
# =============================================================

use strict;
use warnings;
use DBIx::DataModel::Meta::Utils qw(define_abstract_methods);
use namespace::clean;

our $VERSION = '1.01';

define_abstract_methods(__PACKAGE__, qw(dispatch_event phase));

sub new {
    my ($class, %args) = @_;
    return bless \%args, $class;
}

1;
