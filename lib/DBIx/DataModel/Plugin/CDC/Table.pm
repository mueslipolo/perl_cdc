package DBIx::DataModel::Plugin::CDC::Table;

use strict;
use warnings;
use parent 'DBIx::DataModel::Source::Table';

use Try::Tiny;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Event;
use namespace::clean;

our $VERSION = '2.00';

# Resolve parent methods once at compile time.
my $SUPER_INSERT = __PACKAGE__->can('DBIx::DataModel::Source::Table::insert');
my $SUPER_UPDATE = __PACKAGE__->can('DBIx::DataModel::Source::Table::update');
my $SUPER_DELETE = __PACKAGE__->can('DBIx::DataModel::Source::Table::delete');

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
    my ($class_or_self, $obj) = @_;

    # Build skip-set: internal keys + composition component role names
    my %skip;
    if (my $metadm = eval { $obj->metadm }) {
        $skip{$_} = 1 for $metadm->components;
    }

    return {
        map  { uc($_) => $obj->{$_} }
        grep { !/^__/ && !$skip{$_} }
        keys %$obj
    };
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
    $args{primary_key} //= [ map { uc($_) } $self->metadm->primary_key ];
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
    my $wantarray = wantarray;
    try {
        @result = $wantarray ? $code->() : (scalar $code->());
        $dbh->commit;
    } catch {
        my $err = $_;
        try { $dbh->rollback };
        die $err;
    };
    return $wantarray ? @result : $result[0];
}

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
        my @results = $SUPER_INSERT->($self, @args);

        for my $rec (@args) {
            next unless ref $rec;
            $self->_cdc_dispatch($self->_cdc_event(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'INSERT',
                row_id      => $self->_cdc_pk_from($rec),
                old_data    => undef,
                new_data    => _cdc_snapshot(undef, $rec),
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
    my $is_class_method = @args && !ref $args[0] && ($args[0] // '') =~ /^-/;

    if ($is_class_method) {
        return $self->_cdc_class_update($schema_class, $tname, $want_old, @args);
    }

    # Instance method: $row->update({...})
    my $old = $want_old ? _cdc_snapshot(undef, $self) : undef;

    return $self->_cdc_ensure_atomic(sub {
        my $result = $SUPER_UPDATE->($self, @args);

        my $to_set  = ref $args[0] eq 'HASH' ? $args[0] : {};
        my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;
        my $new = $old ? { %$old, %changes }
                       : { _cdc_snapshot(undef, $self), %changes };

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
            my @snapshots = map { _cdc_snapshot(undef, $_) } @$rows;

            my $result = $SUPER_UPDATE->($self, @args);

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

        my $result = $SUPER_UPDATE->($self, @args);

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
    my $is_class_method = @args && !ref $args[0] && ($args[0] // '') =~ /^-/;

    if ($is_class_method) {
        return $self->_cdc_class_delete($schema_class, $tname, $want_old, @args);
    }

    # Instance method: $row->delete()
    my $old = $want_old ? _cdc_snapshot(undef, $self) : undef;

    return $self->_cdc_ensure_atomic(sub {
        my $result = $SUPER_DELETE->($self, @args);

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
            my @snapshots = map { _cdc_snapshot(undef, $_) } @$rows;

            my $result = $SUPER_DELETE->($self, @args);

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

        my $result = $SUPER_DELETE->($self, @args);

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

=head1 DESCRIPTION

Use as C<table_parent> when declaring a DBIx::DataModel schema.
Overrides C<insert>, C<update>, and C<delete> to capture change
events and dispatch them to registered listeners.

=cut
