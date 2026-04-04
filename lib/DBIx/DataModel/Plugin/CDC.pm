package DBIx::DataModel::Plugin::CDC;

use strict;
use warnings;
use Carp qw(croak);
use Scalar::Util qw(refaddr);
use Cpanel::JSON::XS ();
use namespace::clean;

our $VERSION = '2.00';

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

    my $tables_arg = $args{tables} // 'all';
    my %tracked;

    if ($tables_arg eq 'all') {
        $tracked{ $_->name } = 1 for $schema_class->metadm->tables;
    } elsif (ref $tables_arg eq 'ARRAY') {
        $tracked{$_} = 1 for @$tables_arg;
    } else {
        croak "tables must be 'all' or an arrayref";
    }

    $REGISTRY{$schema_class} = {
        tracked     => \%tracked,
        capture_old => $args{capture_old} // 0,
        listeners   => [],
        dbi_table   => undef,
        _sth_cache  => {},
    };

    return $class;
}

sub config_for {
    my ($class, $schema_class) = @_;
    return $REGISTRY{$schema_class};
}

sub is_tracked {
    my ($class, $schema_class, $table_name) = @_;
    my $cfg = $REGISTRY{$schema_class} or return 0;
    return $cfg->{tracked}{$table_name} ? 1 : 0;
}

sub capture_old {
    my ($class, $schema_class) = @_;
    my $cfg = $REGISTRY{$schema_class} or return 1;
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
            $event->{operation}, $event->{event_id};
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
                _call_safe($_, $event, $schema_obj) for @async;
            });
        } else {
            _call_safe($_, $event, $schema_obj) for @async;
        }
    }
}

# ---------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------

sub _call_safe {
    my ($listener, $event, $schema) = @_;
    my $ok = eval { $listener->{cb}->($event, $schema); 1 };
    return if $ok;
    my $err = $@;
    die $err  if $listener->{on_error} eq 'abort';
    warn "CDC listener failed: $err" if $listener->{on_error} eq 'warn';
    # ignore: silent (logs with CDC_DEBUG)
    warn "CDC listener failed (ignored): $err"
        if $listener->{on_error} eq 'ignore' && $ENV{CDC_DEBUG};
}

sub _get_cached_sth {
    my ($cfg, $dbh, $table) = @_;
    my $addr = refaddr($dbh);
    my $gen  = $dbh->{dbi_connect_generation} // 0;
    my $cached = $cfg->{_sth_cache}{$addr};

    if ($cached && $cached->{gen} == $gen) {
        return $cached->{sth};
    }

    my $sth = $dbh->prepare(
        qq{INSERT INTO $table (table_name, operation, old_data, new_data)
           VALUES (?, ?, ?, ?)}
    );
    $cfg->{_sth_cache}{$addr} = { sth => $sth, gen => $gen };
    return $sth;
}

# ---------------------------------------------------------------
# Query helpers (require log_to_dbi to have been called)
# ---------------------------------------------------------------

sub _dbi_table {
    my ($class, $schema_class) = @_;
    my $cfg = $REGISTRY{$schema_class}
        or croak 'CDC not configured';
    return $cfg->{dbi_table}
        or croak 'No DBI logging configured — call log_to_dbi() first';
}

sub events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'events_for: "table" argument required' unless $args{table};

    my $tbl   = $class->_dbi_table($schema_class);
    my $dbh   = $schema_class->singleton->dbh
        or croak 'No active database connection';
    my $table = uc $args{table};
    my $op    = defined $args{operation} ? uc $args{operation} : undef;

    my $sql  = "SELECT * FROM $tbl WHERE table_name = ?";
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
    $schema_class->singleton->dbh->do("DELETE FROM $tbl");
}

sub clear_events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'table required' unless $args{table};
    my $tbl = $class->_dbi_table($schema_class);
    $schema_class->singleton->dbh->do(
        "DELETE FROM $tbl WHERE table_name = ?",
        undef, uc $args{table},
    );
}

sub event_pairs {
    my ($class, $schema_class, %args) = @_;
    $args{operation} = 'UPDATE';
    my $events = $class->events_for($schema_class, %args);
    return [
        map {
            my $old = $_->{old_data};
            my $new = $_->{new_data};
            $old = $JSON_DECODE->decode($old) if defined $old && !ref $old;
            $new = $JSON_DECODE->decode($new) if defined $new && !ref $new;
            [$old // {}, $new // {}]
        } @$events
    ];
}

1;

__END__

=head1 NAME

DBIx::DataModel::Plugin::CDC - Change Data Capture for DBIx::DataModel

=head1 SYNOPSIS

    use DBIx::DataModel::Plugin::CDC;

    # Configure
    DBIx::DataModel::Plugin::CDC
        ->setup('App::Schema', tables => 'all')
        ->log_to_dbi('App::Schema', 'cdc_events')
        ->log_to_stderr('App::Schema')
        ->on('App::Schema', insert => sub {
            my ($event, $schema) = @_;
            # push to Redis, webhook, etc.
        })
        ->on('App::Schema', '*' => sub {
            my ($event) = @_;
            # called for every operation
        }, { phase => 'in_transaction', on_error => 'abort' });

=head1 DESCRIPTION

Captures INSERT, UPDATE, and DELETE events by wrapping
L<DBIx::DataModel> table methods via C<table_parent>.

Events are dispatched to listeners registered with C<on()>.
Built-in shortcuts C<log_to_dbi()> and C<log_to_stderr()>
cover the common cases without any handler classes.

=cut
