package DBIx::DataModel::Plugin::CDC::Handler::DBI;

use strict;
use warnings;
use Carp qw(croak);
use Cpanel::JSON::XS ();

my $JSON = Cpanel::JSON::XS->new->utf8->canonical->allow_nonref;

sub new {
    my ($class, %args) = @_;
    return bless {
        table_name => $args{table_name} // 'cdc_events',
        _sth_cache => {},   # dbh addr => prepared $sth
    }, $class;
}

sub phase { 'in_transaction' }

sub dispatch_event {
    my ($self, $event, $schema) = @_;

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

# Prepare once per $dbh, reuse across calls.
sub _get_sth {
    my ($self, $dbh) = @_;
    my $addr = "$dbh";   # stringified ref as cache key
    my $sth  = $self->{_sth_cache}{$addr};
    return $sth if $sth && $sth->{Active};

    my $table = $self->{table_name};
    $sth = $dbh->prepare(
        qq{INSERT INTO $table (table_name, operation, old_data, new_data)
           VALUES (?, ?, ?, ?)}
    );
    $self->{_sth_cache}{$addr} = $sth;
    return $sth;
}

1;
