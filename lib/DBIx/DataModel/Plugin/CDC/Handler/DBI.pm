package DBIx::DataModel::Plugin::CDC::Handler::DBI;

use strict;
use warnings;
use parent 'DBIx::DataModel::Plugin::CDC::Handler';

use Carp qw(croak);
use Scalar::Util qw(refaddr);
use Params::Validate qw(validate_with SCALAR);
use Cpanel::JSON::XS ();
use namespace::clean;

our $VERSION = '1.01';

my $JSON = Cpanel::JSON::XS->new->utf8->canonical->allow_nonref;

my $new_spec = {
    table_name => { type => SCALAR, default => 'cdc_events',
                    regex => qr/\A[a-zA-Z_]\w*\z/ },
};

sub new {
    my $class = shift;
    my %args = validate_with(params => \@_, spec => $new_spec);
    return bless {
        table_name  => $args{table_name},
        _sth_cache  => {},
    }, $class;
}

sub phase { 'in_transaction' }

sub table_name { $_[0]->{table_name} }

sub dispatch_event {
    my ($self, $event, $schema) = @_;
    croak 'dispatch_event: schema argument required' unless $schema;

    my $dbh = $schema->dbh;
    my $old_json = defined $event->{old_data}
        ? $JSON->encode($event->{old_data}) : undef;
    my $new_json = defined $event->{new_data}
        ? $JSON->encode($event->{new_data}) : undef;

    my $sth = $self->_get_sth($dbh);
    $sth->execute(
        $event->{table_name},
        $event->{operation},
        $old_json,
        $new_json,
    );
}

sub _get_sth {
    my ($self, $dbh) = @_;
    my $addr = refaddr($dbh);
    my $gen  = $dbh->{dbi_connect_generation} // 0;
    my $cached = $self->{_sth_cache}{$addr};

    if ($cached && $cached->{gen} == $gen) {
        return $cached->{sth};
    }

    my $table = $self->{table_name};
    my $sth = $dbh->prepare(
        qq{INSERT INTO $table (table_name, operation, old_data, new_data)
           VALUES (?, ?, ?, ?)}
    );
    $self->{_sth_cache}{$addr} = { sth => $sth, gen => $gen };
    return $sth;
}

1;
