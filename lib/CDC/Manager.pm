package CDC::Manager;

# =============================================================
# CDC::Manager  –  Application-level Change Data Capture
#
# Installs ORM hooks on DBIx::DataModel table classes to capture
# INSERT, UPDATE, and DELETE events into the CDC_EVENTS table.
#
# No database triggers or stored procedures required.
# Portable across any DBI-supported database.
# =============================================================

use strict;
use warnings;
use Carp qw(croak);
use Class::Method::Modifiers qw(around);
use Scalar::Util qw(blessed);

our $VERSION = '0.02';

# ---------------------------------------------------------------
# new(%args)
#   dbh => $dbh   (required) live DBI database handle
# ---------------------------------------------------------------
sub new {
    my ($class, %args) = @_;
    croak 'CDC::Manager->new requires a "dbh" argument'
        unless $args{dbh};
    return bless {
        dbh     => $args{dbh},
        tracked => {},
    }, $class;
}

# ---------------------------------------------------------------
# track_table($table_class, $table_name)
#
# Installs around-method hooks on $table_class for insert,
# update, and delete.  $table_name is the logical name stored
# in cdc_events.table_name (upper-cased).
# ---------------------------------------------------------------
sub track_table {
    my ($self, $table_class, $table_name) = @_;
    croak 'track_table requires (table_class, table_name)'
        unless $table_class && $table_name;

    my $tname = uc $table_name;
    $self->{tracked}{$table_class} = $tname;

    my $cdc = $self;   # closure capture

    # --- INSERT hook (class method) ---
    around "${table_class}::insert" => sub {
        my ($orig, $invocant, @args) = @_;
        my @results = $invocant->$orig(@args);

        # $orig blesses each record hashref into $table_class.
        # After insert, auto-generated columns (id, defaults) are
        # populated on the blessed record.  Walk @args to find them.
        for my $rec (@args) {
            next unless ref $rec eq 'HASH' || blessed($rec);
            # Skip option strings like -returning
            next unless ref $rec;
            $cdc->_write_event($tname, 'INSERT', undef, $rec);
        }

        return wantarray ? @results : $results[0];
    };

    # --- UPDATE hook (instance method) ---
    around "${table_class}::update" => sub {
        my ($orig, $self_row, @args) = @_;

        # Snapshot the before-state from the loaded row object
        my %old = $cdc->_snapshot($self_row);

        my $result = $self_row->$orig(@args);

        # Build new-state: old + applied changes (upper-case keys)
        my $to_set = ref $args[0] eq 'HASH' ? $args[0] : {};
        my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;
        my %new = (%old, %changes);

        $cdc->_write_event($tname, 'UPDATE', \%old, \%new);
        return $result;
    };

    # --- DELETE hook (instance method) ---
    around "${table_class}::delete" => sub {
        my ($orig, $self_row, @args) = @_;

        my %old = $cdc->_snapshot($self_row);

        my $result = $self_row->$orig(@args);

        $cdc->_write_event($tname, 'DELETE', \%old, undef);
        return $result;
    };

    return $self;
}

# ---------------------------------------------------------------
# _snapshot($row_object) -> %hash
#
# Extracts column data from a blessed DBIx::DataModel row,
# skipping internal keys (prefixed with __).
# ---------------------------------------------------------------
sub _snapshot {
    my ($self, $obj) = @_;
    return map  { uc($_) => $obj->{$_} }
           grep { !/^__/ }
           keys %$obj;
}

# ---------------------------------------------------------------
# _build_row_image(\%hash) -> $string
#
# Serialises a hash into pipe-delimited KEY=VALUE format.
# NULL values use the sentinel literal 'NULL'.
# Keys are sorted for deterministic output.
# ---------------------------------------------------------------
sub _build_row_image {
    my ($self, $href) = @_;
    return undef unless $href;

    return join '|',
        map { $_ . '=' . (defined $href->{$_} ? $href->{$_} : 'NULL') }
        sort keys %$href;
}

# ---------------------------------------------------------------
# _write_event($table, $operation, \%old, \%new)
#
# Inserts a row into cdc_events.
# ---------------------------------------------------------------
sub _write_event {
    my ($self, $table, $op, $old_href, $new_href) = @_;

    my $old_img = $self->_build_row_image($old_href);
    my $new_img = $self->_build_row_image(
        $new_href ? { $self->_snapshot($new_href) } : undef
    );

    $self->{dbh}->do(
        q{INSERT INTO cdc_events (table_name, operation, old_data, new_data)
          VALUES (?, ?, ?, ?)},
        undef, $table, $op, $old_img, $new_img,
    );
}

# ---------------------------------------------------------------
# events_for(%args) -> \@events
# ---------------------------------------------------------------
sub events_for {
    my ($self, %args) = @_;
    croak 'events_for: "table" argument is required'
        unless defined $args{table};

    my $table = uc $args{table};
    my $op    = defined $args{operation} ? uc $args{operation} : undef;

    my $sql = q{
        SELECT event_id, event_time, table_name, operation,
               old_data, new_data, session_user, transaction_id
        FROM   cdc_events
        WHERE  table_name = ?
    };
    my @bind = ($table);

    if (defined $op) {
        $sql .= ' AND operation = ?';
        push @bind, $op;
    }
    $sql .= ' ORDER BY event_id ASC';

    return $self->{dbh}->selectall_arrayref($sql, { Slice => {} }, @bind);
}

# ---------------------------------------------------------------
# count_events(%args) -> $n
# ---------------------------------------------------------------
sub count_events {
    my ($self, %args) = @_;
    return scalar @{ $self->events_for(%args) };
}

# ---------------------------------------------------------------
# latest_event(%args) -> \%event | undef
# ---------------------------------------------------------------
sub latest_event {
    my ($self, %args) = @_;
    my $events = $self->events_for(%args);
    return @$events ? $events->[-1] : undef;
}

# ---------------------------------------------------------------
# parse_row_image($string) -> \%hash
#
# Parses pipe-delimited KEY=VALUE.  'NULL' sentinel -> undef.
# ---------------------------------------------------------------
sub parse_row_image {
    my ($self, $raw) = @_;
    return {} unless defined $raw && $raw ne '';

    my %row;
    for my $pair (split /\|/, $raw) {
        my ($key, $val) = split /=/, $pair, 2;
        next unless defined $key && length $key;
        $row{$key} = (defined $val && $val eq 'NULL') ? undef : $val;
    }
    return \%row;
}

# ---------------------------------------------------------------
# clear_events / clear_events_for
# ---------------------------------------------------------------
sub clear_events {
    my ($self) = @_;
    $self->{dbh}->do('DELETE FROM cdc_events');
    return;
}

sub clear_events_for {
    my ($self, %args) = @_;
    croak 'clear_events_for: "table" argument is required'
        unless defined $args{table};
    $self->{dbh}->do(
        'DELETE FROM cdc_events WHERE table_name = ?',
        undef, uc $args{table}
    );
    return;
}

# ---------------------------------------------------------------
# event_pairs(%args) -> [ [$old_href, $new_href], ... ]
# ---------------------------------------------------------------
sub event_pairs {
    my ($self, %args) = @_;
    $args{operation} = 'UPDATE';
    my $events = $self->events_for(%args);
    return [
        map { [
            $self->parse_row_image($_->{old_data}),
            $self->parse_row_image($_->{new_data}),
        ] } @$events
    ];
}

1;

__END__

=head1 NAME

CDC::Manager – Application-level Change Data Capture via ORM hooks

=head1 SYNOPSIS

    use CDC::Manager;
    my $cdc = CDC::Manager->new(dbh => $dbh);

    # Install hooks on table classes
    $cdc->track_table('App::Schema::Department', 'departments');
    $cdc->track_table('App::Schema::Employee',   'employees');

    # After ORM operations, query captured events
    my $events = $cdc->events_for(table => 'employees');
    my $last   = $cdc->latest_event(table => 'employees', operation => 'UPDATE');
    my $row    = $cdc->parse_row_image($last->{new_data});

=head1 DESCRIPTION

Captures INSERT, UPDATE, and DELETE events by wrapping
L<DBIx::DataModel> table methods.  No database triggers required.

Trade-off: only captures changes made through the ORM.
Raw DBI statements bypass the hooks.

=cut
