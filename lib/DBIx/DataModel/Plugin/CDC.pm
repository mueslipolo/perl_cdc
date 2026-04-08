package DBIx::DataModel::Plugin::CDC;

use strict;
use warnings;
use Carp qw(croak carp);
use Try::Tiny;
use Cpanel::JSON::XS ();
use namespace::clean;

our $VERSION = '2.00';

# Thread caveat: Cpanel::JSON::XS objects carry internal C state and
# are not safe to share across Perl ithreads.  This is fine for the
# standard single-threaded forking deployment model.
my $JSON_ENCODE = Cpanel::JSON::XS->new->utf8->canonical->allow_nonref;
my $JSON_DECODE = Cpanel::JSON::XS->new->canonical->allow_nonref;

# Package-level registry: schema_class => \%config
my %REGISTRY;

# ---------------------------------------------------------------
# setup($schema_class, %args)
#
#   tables      => 'all' | \@table_names
#   capture_old => 0 | 1    (default: 0)
#
# When capture_old is 0 (the default), UPDATE and DELETE events
# have old_data and changed_columns set to undef.  Class-method
# update/delete skip the pre-fetch SELECT (zero overhead).
# Set to 1 if you need before/after comparison.
# ---------------------------------------------------------------
sub setup {
    my ($class, $schema_class, %args) = @_;
    croak 'setup() requires a schema class name' unless $schema_class;

    if (my $prev = $REGISTRY{$schema_class}) {
        if (!$args{force} && @{ $prev->{listeners} || [] }) {
            carp "CDC: setup() called again for $schema_class"
                . " — previous listeners discarded (pass force => 1 to suppress)";
        }
    }

    my $tables_arg = $args{tables} // 'all';
    my %tracked;

    if ($tables_arg eq 'all') {
        for my $table ($schema_class->metadm->tables) {
            $tracked{ $table->name } = 1;
        }
    } elsif (ref $tables_arg eq 'ARRAY') {
        for my $name (@$tables_arg) {
            $tracked{$name} = 1;
        }
    } else {
        croak "tables must be 'all' or an arrayref";
    }

    $REGISTRY{$schema_class} = {
        tracked     => \%tracked,
        capture_old => $args{capture_old} // 0,
        listeners   => [],
        dbi_table   => undef,
    };

    return $class;
}

sub config_for {
    my ($class, $schema_class) = @_;
    my $cfg = $REGISTRY{$schema_class} or return undef;
    return { %$cfg };
}

sub is_tracked {
    my ($class, $schema_class, $table_name) = @_;
    my $cfg = $REGISTRY{$schema_class} or return 0;
    return $cfg->{tracked}{$table_name} ? 1 : 0;
}

sub capture_old {
    my ($class, $schema_class) = @_;
    my $cfg = $REGISTRY{$schema_class} or return 0;
    return $cfg->{capture_old};
}

# ---------------------------------------------------------------
# on($schema_class, $operation, $coderef, \%opts?)
#
# Register a listener.  $operation is 'insert', 'update',
# 'delete', or '*' (all).
#
# Options:
#   phase    => 'in_transaction' | 'post_commit'  (default: post_commit)
#   on_error => 'abort' | 'warn' | 'ignore'       (default: warn)
#
# Returns $class for chaining.
# ---------------------------------------------------------------
sub on {
    my ($class, $schema_class, $operation, $cb, $opts) = @_;
    croak 'on() requires a schema class'  unless $schema_class;
    croak 'on() requires an operation'    unless $operation;
    croak "invalid operation: $operation"
        unless $operation =~ /\A(?:\*|INSERT|UPDATE|DELETE)\z/i;
    croak 'on() requires a coderef'       unless ref $cb eq 'CODE';

    $opts //= {};
    my $phase    = $opts->{phase}    // 'post_commit';
    my $on_error = $opts->{on_error} // 'warn';

    croak "invalid phase: $phase"
        unless $phase =~ /\A(?:in_transaction|post_commit)\z/;
    croak "invalid on_error: $on_error"
        unless $on_error =~ /\A(?:abort|warn|ignore)\z/;

    my $cfg = $REGISTRY{$schema_class}
        or croak "CDC not configured for $schema_class — call setup() first";

    push @{ $cfg->{listeners} }, {
        operation => uc $operation,
        cb        => $cb,
        phase     => $phase,
        on_error  => $on_error,
    };

    return $class;
}

# ---------------------------------------------------------------
# log_to_dbi($schema_class, $table_name?)
#
# Built-in: persist events as JSON to a database table.
# Registers an in_transaction listener with abort-on-error.
# Uses a prepared statement cache for performance.
# ---------------------------------------------------------------
sub log_to_dbi {
    my ($class, $schema_class, $table_name) = @_;
    $table_name //= 'cdc_events';
    croak "Invalid table name: $table_name"
        unless $table_name =~ /\A[a-zA-Z_]\w*\z/;

    my $cfg = $REGISTRY{$schema_class}
        or croak "CDC not configured for $schema_class";
    $cfg->{dbi_table} = $table_name;

    $class->on($schema_class, '*' => sub {
        my ($event, $schema) = @_;
        my $dbh = $schema->dbh;

        my $sth = _get_cached_sth($cfg, $dbh, $table_name);
        $sth->execute(
            $event->{table_name},
            $event->{operation},
            defined $event->{old_data}
                ? $JSON_ENCODE->encode($event->{old_data}) : undef,
            defined $event->{new_data}
                ? $JSON_ENCODE->encode($event->{new_data}) : undef,
        );
    }, { phase => 'in_transaction', on_error => 'abort' });

    return $class;
}

# ---------------------------------------------------------------
# log_to_stderr($schema_class, $prefix?)
#
# Built-in: print one-line structured log per event.
# ---------------------------------------------------------------
sub log_to_stderr {
    my ($class, $schema_class, $prefix) = @_;
    $prefix //= 'CDC';

    $class->on($schema_class, '*' => sub {
        my ($event) = @_;
        warn sprintf "[%s] %s %s %s\n",
            $prefix, $event->{table_name},
            $event->{operation}, $event->{cdc_event_id};
    }, { phase => 'post_commit', on_error => 'ignore' });

    return $class;
}

# ---------------------------------------------------------------
# dispatch($schema_class, $schema_obj, $event)
#
# Called by CDC::Table after each DML.  Routes events to
# matching listeners by operation and phase.
# ---------------------------------------------------------------
sub dispatch {
    my ($class, $schema_class, $schema_obj, $event) = @_;
    my $cfg = $REGISTRY{$schema_class} or return;

    my $op = uc($event->{operation});
    my @async;

    for my $l (@{ $cfg->{listeners} }) {
        next unless $l->{operation} eq '*' || $l->{operation} eq $op;

        if ($l->{phase} eq 'in_transaction') {
            _call_safe($l, $event, $schema_obj);
        } else {
            push @async, $l;
        }
    }

    if (@async) {
        if ($schema_obj->{transaction_dbhs}) {
            $schema_obj->do_after_commit(sub {
                for my $listener (@async) {
                    _call_safe($listener, $event, $schema_obj);
                }
            });
        } else {
            for my $listener (@async) {
                _call_safe($listener, $event, $schema_obj);
            }
        }
    }
}

# ---------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------

sub _call_safe {
    my ($listener, $event, $schema) = @_;
    try {
        $listener->{cb}->($event, $schema);
    } catch {
        my $err = $_;
        die $err if $listener->{on_error} eq 'abort';
        warn "CDC listener failed: $err"            if $listener->{on_error} eq 'warn';
        warn "CDC listener failed (ignored): $err"  if $listener->{on_error} eq 'ignore';
    };
}

sub _get_cached_sth {
    my ($cfg, $dbh, $table) = @_;
    my $key = "private_cdc_sth_$table";
    return $dbh->{$key} if $dbh->{$key};

    my $qt = $dbh->quote_identifier($table);
    my $sth = $dbh->prepare(
        qq{INSERT INTO $qt (table_name, operation, old_data, new_data)
           VALUES (?, ?, ?, ?)}
    );
    $dbh->{$key} = $sth;
    return $sth;
}

# ---------------------------------------------------------------
# Query helpers (require log_to_dbi to have been called)
# ---------------------------------------------------------------

sub _dbi_table {
    my ($class, $schema_class) = @_;
    my $cfg = $REGISTRY{$schema_class}
        or croak 'CDC not configured';
    my $table = $cfg->{dbi_table}
        or croak 'No DBI logging configured — call log_to_dbi() first';
    return $table;
}

sub events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'events_for: "table" argument required' unless $args{table};

    my $tbl   = $class->_dbi_table($schema_class);
    my $dbh   = $schema_class->singleton->dbh
        or croak 'No active database connection';
    my $table = uc $args{table};
    my $op    = defined $args{operation} ? uc $args{operation} : undef;

    my $qt   = $dbh->quote_identifier($tbl);
    my $sql  = "SELECT * FROM $qt WHERE table_name = ?";
    my @bind = ($table);
    if (defined $op) {
        $sql .= ' AND operation = ?';
        push @bind, $op;
    }
    $sql .= ' ORDER BY event_id ASC';

    return $dbh->selectall_arrayref($sql, { Slice => {} }, @bind);
}

sub count_events {
    my ($class, $schema_class, %args) = @_;
    return scalar @{ $class->events_for($schema_class, %args) };
}

sub latest_event {
    my ($class, $schema_class, %args) = @_;
    my $events = $class->events_for($schema_class, %args);
    return @$events ? $events->[-1] : undef;
}

sub clear_events {
    my ($class, $schema_class) = @_;
    my $tbl = $class->_dbi_table($schema_class);
    my $dbh = $schema_class->singleton->dbh
        or croak 'No active database connection';
    my $qt  = $dbh->quote_identifier($tbl);
    $dbh->do("DELETE FROM $qt");
}

sub clear_events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'table required' unless $args{table};
    my $tbl = $class->_dbi_table($schema_class);
    my $dbh = $schema_class->singleton->dbh
        or croak 'No active database connection';
    my $qt  = $dbh->quote_identifier($tbl);
    $dbh->do(
        "DELETE FROM $qt WHERE table_name = ?",
        undef, uc $args{table},
    );
}

sub event_pairs {
    my ($class, $schema_class, %args) = @_;
    $args{operation} = 'UPDATE';
    my $events = $class->events_for($schema_class, %args);
    my @pairs;
    for my $ev (@$events) {
        my $old = $ev->{old_data};
        my $new = $ev->{new_data};
        try {
            $old = $JSON_DECODE->decode($old) if defined $old && !ref $old;
            $new = $JSON_DECODE->decode($new) if defined $new && !ref $new;
        } catch {
            croak "CDC: malformed JSON in event row $ev->{event_id}: $_";
        };
        push @pairs, [$old // {}, $new // {}];
    }
    return \@pairs;
}

1;

__END__

=head1 NAME

DBIx::DataModel::Plugin::CDC - Change Data Capture for DBIx::DataModel

=head1 SYNOPSIS

    use DBIx::DataModel::Plugin::CDC;

    DBIx::DataModel::Plugin::CDC
        ->setup('App::Schema',
            tables      => 'all',
            capture_old => 0,        # default; set 1 for before/after diff
        )
        ->log_to_dbi('App::Schema', 'cdc_events')
        ->on('App::Schema', '*' => sub {
            my ($event, $schema) = @_;
            # $event->{row_id}      — { ID => 42 } always present
            # $event->{primary_key} — ['ID'] column names
            # $event->{new_data}    — changed data (or full row for INSERT)
            # $event->{old_data}    — undef unless capture_old => 1
        });

=head1 DESCRIPTION

Captures INSERT, UPDATE, and DELETE events by overriding
L<DBIx::DataModel> table methods via C<table_parent>.

Events are dispatched to listeners registered with C<on()>.
Built-in shortcuts C<log_to_dbi()> and C<log_to_stderr()>
cover the common cases.

=head2 setup($schema_class, %opts)

    tables      => 'all' | \@names    # which tables to track
    capture_old => 0 | 1              # capture old_data (default: 0)

=head2 on($schema_class, $operation, \&callback, \%opts?)

    $operation: 'INSERT', 'UPDATE', 'DELETE', or '*'
    phase:      'in_transaction' | 'post_commit' (default)
    on_error:   'abort' | 'warn' (default) | 'ignore'

=head2 log_to_dbi($schema_class, $table_name?)

Persist events as JSON.  Runs in_transaction, abort on error.

=head2 log_to_stderr($schema_class, $prefix?)

Print one-line log.  Runs post_commit.

=head2 dispatch($schema_class, $schema_obj, $event)

Called internally by C<CDC::Table> after each DML.  Routes
events to matching listeners by operation and phase.  Not
normally called by user code.

=head2 Query Helpers

These methods require C<log_to_dbi()> to have been called.

=over 4

=item events_for($schema_class, table => $name, operation => $op?)

Returns an arrayref of event rows (hashrefs) from the CDC table.
C<operation> is optional.  Rows are ordered by C<event_id ASC>.

=item count_events($schema_class, table => $name, operation => $op?)

Returns the number of matching events.

=item latest_event($schema_class, table => $name, operation => $op?)

Returns the most recent matching event row, or C<undef>.

=item event_pairs($schema_class, table => $name)

Returns UPDATE events as C<[[\%old, \%new], ...]> with JSON-decoded
C<old_data>/C<new_data>.

=item clear_events($schema_class)

Deletes all rows from the CDC table.

=item clear_events_for($schema_class, table => $name)

Deletes CDC rows for a specific table.

=back

=head2 Event Envelope

    {
        cdc_event_id, occurred_at, schema_name, table_name,
        primary_key => ['ID'],        # PK column names
        row_id      => { ID => 42 },  # actual PK values
        operation   => 'UPDATE',
        old_data    => \%hash | undef,
        new_data    => \%hash | undef,
        changed_columns => [...] | undef,
    }

C<row_id> is always present.  C<old_data> and C<changed_columns>
require C<capture_old =E<gt> 1>.

=cut
