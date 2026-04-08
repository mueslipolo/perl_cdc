package DBIx::DataModel::Plugin::CDC::Table;

use strict;
use warnings;
use parent 'DBIx::DataModel::Source::Table';

use Carp qw(croak);
use Try::Tiny;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Event;
use namespace::clean;

our $VERSION = '2.00';

# ---------------------------------------------------------------
# CDC naming convention: all column names and table names in event
# data are UPPER-CASED.  Normalization points:
#   - _cdc_snapshot()    : column keys  -> uc()
#   - _cdc_table_name()  : table name   -> uc(db_from)
#   - _cdc_pk_from()     : PK keys      -> uc()
#   - _cdc_event()       : primary_key  -> uc()
# This matches Oracle's default behavior (NAME_uc) and is applied
# uniformly regardless of the backend's FetchHashKeyName setting.
# ---------------------------------------------------------------

# Helper: detect class-method style calls like ->update(-set => ..., -where => ...)
sub _cdc_is_named_args {
    my (@args) = @_;
    return 0 unless @args;
    return 0 if ref $args[0];
    return ($args[0] // '') =~ /^-/ ? 1 : 0;
}

# ---------------------------------------------------------------
# _cdc_table_name() -> $name | undef
# ---------------------------------------------------------------
sub _cdc_table_name {
    my ($self) = @_;
    my $schema_class = $self->schema->metadm->class;
    my $table_name   = $self->metadm->name;
    return DBIx::DataModel::Plugin::CDC->is_tracked($schema_class, $table_name)
        ? uc($self->metadm->db_from)
        : undef;
}

# ---------------------------------------------------------------
# _cdc_snapshot($obj) -> \%hash
#
# Captures all column values, including inflated refs (multivalue
# fields, JSON columns, etc).  Skips only DBIx::DataModel internals
# (__schema, __schema_class) and Composition component keys.
# ---------------------------------------------------------------
sub _cdc_snapshot {
    my ($obj) = @_;

    # Build skip-set: internal keys + composition component role names
    my %skip;
    if ($obj->can('metadm')) {
        my $metadm = $obj->metadm;
        for my $comp ($metadm->components) {
            $skip{$comp} = 1;
        }
    }

    my %snapshot;
    for my $key (keys %$obj) {
        next if $key =~ /^__/;
        next if $skip{$key};
        $snapshot{ uc($key) } = $obj->{$key};
    }
    return \%snapshot;
}

sub _cdc_dispatch {
    my ($self, $event) = @_;
    my $schema       = $self->schema;
    my $schema_class = $schema->metadm->class;
    DBIx::DataModel::Plugin::CDC->dispatch($schema_class, $schema, $event);
}

# Shortcut: build event with primary_key auto-populated
sub _cdc_event {
    my ($self, %args) = @_;
    if (!defined $args{primary_key}) {
        my @pk_cols = $self->metadm->primary_key;
        $args{primary_key} = [ map { uc($_) } @pk_cols ];
    }
    return DBIx::DataModel::Plugin::CDC::Event->build(%args);
}

# ---------------------------------------------------------------
# _cdc_pk_from($obj) -> \%pk_hash
#
# Extracts primary key columns from a row object.  Keys upper-cased.
# ---------------------------------------------------------------
sub _cdc_pk_from {
    my ($self, $obj) = @_;
    my @pk_cols = $self->metadm->primary_key;
    return { map { uc($_) => $obj->{$_} } @pk_cols };
}

# ---------------------------------------------------------------
# _cdc_fetch_pks($where) -> \@pk_hashes
#
# Lightweight SELECT of just the PK columns for class-method ops
# when capture_old is off.  Much cheaper than SELECT *.
# ---------------------------------------------------------------
sub _cdc_fetch_pks {
    my ($self, $where) = @_;
    my @pk_cols = $self->metadm->primary_key;
    croak sprintf('Table %s has no primary key; CDC requires one',
        $self->metadm->name) unless @pk_cols;
    my $dbh     = $self->schema->dbh;
    my $table   = $self->metadm->db_from;
    my $sqla    = $self->schema->sql_abstract;

    my ($sql, @bind) = $sqla->select(
        -from    => $table,
        -columns => \@pk_cols,
        -where   => $where,
    );
    return $dbh->selectall_arrayref($sql, { Slice => {} }, @bind);
}

sub _cdc_capture_old {
    my ($self) = @_;
    return DBIx::DataModel::Plugin::CDC->capture_old(
        $self->schema->metadm->class);
}

sub _cdc_in_transaction {
    my ($self) = @_;
    return $self->schema->{transaction_dbhs} ? 1 : 0;
}

# ---------------------------------------------------------------
# _cdc_ensure_atomic($coderef)
# ---------------------------------------------------------------
sub _cdc_ensure_atomic {
    my ($self, $code) = @_;
    my $dbh = $self->schema->dbh;

    if ($self->_cdc_in_transaction || !$dbh->{AutoCommit}) {
        return $code->();
    }

    local $dbh->{AutoCommit} = 0;
    local $dbh->{RaiseError} = 1;
    my @result;
    # wantarray must be captured here — inside the try block it
    # would reflect the try's own calling context, not the caller's.
    my $wantarray = wantarray;
    try {
        @result = $wantarray ? $code->() : (scalar $code->());
        $dbh->commit;
    } catch {
        my $err = $_;
        eval { $dbh->rollback };
        warn "CDC: rollback failed: $@" if $@;
        die $err;
    };
    return $wantarray ? @result : $result[0];
}

# ---------------------------------------------------------------
# Method dispatch note:
#
#   Passthrough (untracked table) uses $self->next::method(@args).
#   This walks the full C3 MRO, which is correct when a user's table
#   class inherits from CDC::Table and may have intermediate classes.
#
#   Inside _cdc_ensure_atomic closures we use $self->SUPER::insert (etc.)
#   because next::method cannot resolve from inside an anonymous sub —
#   it relies on __SUB__ / caller context that closures do not provide.
#   SUPER:: is resolved at compile time to our parent (Source::Table),
#   which is correct as long as CDC::Table is the direct parent.
#   This is a Perl limitation, not a design choice.
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# insert
# ---------------------------------------------------------------
sub insert {
    my $self = shift;
    my @args = @_;

    my $tname = $self->_cdc_table_name;
    return $self->next::method(@args) unless $tname;

    my $schema_class = $self->schema->metadm->class;

    return $self->_cdc_ensure_atomic(sub {
        my @results = $self->SUPER::insert(@args);

        for my $rec (@args) {
            next unless ref $rec;
            $self->_cdc_dispatch($self->_cdc_event(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'INSERT',
                row_id      => $self->_cdc_pk_from($rec),
                old_data    => undef,
                new_data    => _cdc_snapshot($rec),
            ));
        }

        return wantarray ? @results : $results[0];
    });
}

# ---------------------------------------------------------------
# update
# ---------------------------------------------------------------
sub update {
    my $self = shift;
    my @args = @_;

    my $tname = $self->_cdc_table_name;
    return $self->next::method(@args) unless $tname;

    my $schema_class = $self->schema->metadm->class;
    my $want_old     = $self->_cdc_capture_old;
    if (_cdc_is_named_args(@args)) {
        return $self->_cdc_class_update($schema_class, $tname, $want_old, @args);
    }

    # Instance method: $row->update({...})
    my $old = $want_old ? _cdc_snapshot($self) : undef;

    return $self->_cdc_ensure_atomic(sub {
        my $result = $self->SUPER::update(@args);

        my $to_set  = ref $args[0] eq 'HASH' ? $args[0] : {};
        my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;
        my $new = $old ? { %$old, %changes }
                       : { %{_cdc_snapshot($self)}, %changes };

        $self->_cdc_dispatch($self->_cdc_event(
            schema_name => $schema_class,
            table_name  => $tname,
            operation   => 'UPDATE',
            row_id      => $self->_cdc_pk_from($self),
            old_data    => $old,
            new_data    => $new,
        ));

        return $result;
    });
}

sub _cdc_class_update {
    my ($self, $schema_class, $tname, $want_old, @args) = @_;
    my %named   = @args;
    my $to_set  = $named{'-set'}   || {};
    my $where   = $named{'-where'} || {};
    my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;

    if ($want_old) {
        # Full mode: pre-fetch rows for old_data + complete new_data
        my @pk_cols = map { uc($_) } $self->metadm->primary_key;
        return $self->_cdc_ensure_atomic(sub {
            my $rows = $self->select(-where => $where);
            my @snapshots = map { _cdc_snapshot($_) } @$rows;

            my $result = $self->SUPER::update(@args);

            for my $old (@snapshots) {
                my %pk = map { $_ => $old->{$_} } @pk_cols;
                $self->_cdc_dispatch($self->_cdc_event(
                    schema_name => $schema_class,
                    table_name  => $tname,
                    operation   => 'UPDATE',
                    row_id      => \%pk,
                    old_data    => $old,
                    new_data    => { %$old, %changes },
                ));
            }

            return $result;
        });
    }

    # Light mode: fetch only PKs (cheap), one event per row
    return $self->_cdc_ensure_atomic(sub {
        my $pks = $self->_cdc_fetch_pks($where);

        my $result = $self->SUPER::update(@args);

        for my $pk_row (@$pks) {
            my %pk = map { uc($_) => $pk_row->{$_} } keys %$pk_row;
            $self->_cdc_dispatch($self->_cdc_event(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'UPDATE',
                row_id      => \%pk,
                old_data    => undef,
                new_data    => \%changes,
            ));
        }

        return $result;
    });
}

# ---------------------------------------------------------------
# delete
# ---------------------------------------------------------------
sub delete {
    my $self = shift;
    my @args = @_;

    my $tname = $self->_cdc_table_name;
    return $self->next::method(@args) unless $tname;

    my $schema_class = $self->schema->metadm->class;
    my $want_old     = $self->_cdc_capture_old;
    if (_cdc_is_named_args(@args)) {
        return $self->_cdc_class_delete($schema_class, $tname, $want_old, @args);
    }

    # Instance method: $row->delete()
    my $old = $want_old ? _cdc_snapshot($self) : undef;

    return $self->_cdc_ensure_atomic(sub {
        my $result = $self->SUPER::delete(@args);

        $self->_cdc_dispatch($self->_cdc_event(
            schema_name => $schema_class,
            table_name  => $tname,
            operation   => 'DELETE',
            row_id      => $self->_cdc_pk_from($self),
            old_data    => $old,
            new_data    => undef,
        ));

        return $result;
    });
}

sub _cdc_class_delete {
    my ($self, $schema_class, $tname, $want_old, @args) = @_;
    my %named = @args;
    my $where = $named{'-where'} || {};

    if ($want_old) {
        my @pk_cols = map { uc($_) } $self->metadm->primary_key;
        return $self->_cdc_ensure_atomic(sub {
            my $rows = $self->select(-where => $where);
            my @snapshots = map { _cdc_snapshot($_) } @$rows;

            my $result = $self->SUPER::delete(@args);

            for my $old (@snapshots) {
                my %pk = map { $_ => $old->{$_} } @pk_cols;
                $self->_cdc_dispatch($self->_cdc_event(
                    schema_name => $schema_class,
                    table_name  => $tname,
                    operation   => 'DELETE',
                    row_id      => \%pk,
                    old_data    => $old,
                    new_data    => undef,
                ));
            }

            return $result;
        });
    }

    # Light mode: fetch only PKs (cheap), one event per row
    return $self->_cdc_ensure_atomic(sub {
        my $pks = $self->_cdc_fetch_pks($where);

        my $result = $self->SUPER::delete(@args);

        for my $pk_row (@$pks) {
            my %pk = map { uc($_) => $pk_row->{$_} } keys %$pk_row;
            $self->_cdc_dispatch($self->_cdc_event(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'DELETE',
                row_id      => \%pk,
                old_data    => undef,
                new_data    => undef,
            ));
        }

        return $result;
    });
}

1;

__END__

=head1 NAME

DBIx::DataModel::Plugin::CDC::Table - CDC-aware table parent class

=head1 SYNOPSIS

    DBIx::DataModel->Schema('App::Schema',
        table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
    );

=head1 DESCRIPTION

Use as C<table_parent> when declaring a DBIx::DataModel schema.
Overrides C<insert>, C<update>, and C<delete> to capture change
events and dispatch them to registered listeners.

If a table is not tracked (see C<CDC-E<gt>setup()>), the overridden
methods pass through to the parent with zero overhead.

=head2 How DML Interception Works

Each overridden method (C<insert>, C<update>, C<delete>) follows
the same pattern:

=over 4

=item 1. Check if the table is tracked.  If not, call C<next::method>.

=item 2. Wrap the operation in C<_cdc_ensure_atomic> (mini-transaction).

=item 3. Call C<SUPER::insert/update/delete> to execute the real DML.

=item 4. Build an event envelope via C<CDC::Event-E<gt>build()>.

=item 5. Dispatch the event to registered listeners.

=back

B<Why C<next::method> vs C<SUPER::>?>  The passthrough path (step 1)
uses C<next::method> to correctly walk the C3 MRO.  The tracked path
(step 3) runs inside an anonymous sub (the C<_cdc_ensure_atomic>
closure), where C<next::method> cannot resolve the caller — a Perl
limitation.  C<SUPER::> is compile-time-resolved to
C<DBIx::DataModel::Source::Table>, which is correct for the
single-inheritance chain this module uses.

=head2 Instance vs Class-Method Detection

C<update> and C<delete> support two calling conventions in
DBIx::DataModel:

    $row->update({ col => $val });                # instance method
    Table->update(-set => {...}, -where => {...}); # class method

The plugin detects class-method calls by checking whether the first
argument starts with C<-> (e.g., C<-set>, C<-where>).  For class-method
operations, it pre-fetches affected primary keys (or full rows when
C<capture_old =E<gt> 1>) and emits one CDC event per affected row.

=head2 Transaction Safety

C<_cdc_ensure_atomic> wraps the DML + CDC event in a mini-transaction
when C<AutoCommit> is on.  When already inside a transaction (either
C<AutoCommit> off or C<do_transaction>), it lets the enclosing
transaction govern.  This guarantees the DML and CDC event are always
atomic.

=head2 Naming Convention

All column names and table names in CDC event data are B<upper-cased>,
regardless of the database or C<FetchHashKeyName> setting.  The
normalization points are C<_cdc_snapshot> (column keys), C<_cdc_table_name>
(table name), C<_cdc_pk_from> (PK keys), and C<_cdc_event> (primary_key
list).

=cut
