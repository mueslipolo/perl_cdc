package DBIx::DataModel::Plugin::CDC::Handler::DBI;

use strict;
use warnings;
use Carp qw(croak);
use Scalar::Util qw(refaddr);
use Cpanel::JSON::XS ();

my $JSON = Cpanel::JSON::XS->new->utf8->canonical->allow_nonref;

sub new {
    my ($class, %args) = @_;
    my $table = $args{table_name} // 'cdc_events';
    croak "Invalid table name: $table" unless $table =~ /\A[a-zA-Z_]\w*\z/;
    return bless {
        table_name  => $table,
        _sth_cache  => {},   # refaddr => { sth => $sth, gen => $gen }
    }, $class;
}

sub phase { 'in_transaction' }

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

# Prepare once per $dbh, invalidate if the handle changes.
# Uses refaddr + DBI generation counter to detect recycled handles.
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
