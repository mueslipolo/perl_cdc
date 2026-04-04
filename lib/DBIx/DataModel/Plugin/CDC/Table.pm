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
            my $event = DBIx::DataModel::Plugin::CDC::Event->build(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'INSERT',
                old_data    => undef,
                new_data    => _cdc_snapshot(undef, $rec),
            );
            $self->_cdc_dispatch($event);
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
    my $is_class_method = @args && !ref $args[0] && ($args[0] // '') =~ /^-/;

    if ($is_class_method) {
        return $self->_cdc_class_update($schema_class, $tname, @args);
    }

    my $old = _cdc_snapshot(undef, $self);

    return $self->_cdc_ensure_atomic(sub {
        my $result = $SUPER_UPDATE->($self, @args);

        my $to_set  = ref $args[0] eq 'HASH' ? $args[0] : {};
        my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;
        my %new     = (%$old, %changes);

        $self->_cdc_dispatch(DBIx::DataModel::Plugin::CDC::Event->build(
            schema_name => $schema_class,
            table_name  => $tname,
            operation   => 'UPDATE',
            old_data    => $old,
            new_data    => \%new,
        ));

        return $result;
    });
}

sub _cdc_class_update {
    my ($self, $schema_class, $tname, @args) = @_;
    my %named   = @args;
    my $to_set  = $named{'-set'}   || {};
    my $where   = $named{'-where'} || {};

    return $self->_cdc_ensure_atomic(sub {
        my $rows = $self->select(-where => $where);
        my @snapshots = map { _cdc_snapshot(undef, $_) } @$rows;

        my $result = $SUPER_UPDATE->($self, @args);

        my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;
        for my $old (@snapshots) {
            $self->_cdc_dispatch(DBIx::DataModel::Plugin::CDC::Event->build(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'UPDATE',
                old_data    => $old,
                new_data    => { %$old, %changes },
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
    my $is_class_method = @args && !ref $args[0] && ($args[0] // '') =~ /^-/;

    if ($is_class_method) {
        return $self->_cdc_class_delete($schema_class, $tname, @args);
    }

    my $old = _cdc_snapshot(undef, $self);

    return $self->_cdc_ensure_atomic(sub {
        my $result = $SUPER_DELETE->($self, @args);

        $self->_cdc_dispatch(DBIx::DataModel::Plugin::CDC::Event->build(
            schema_name => $schema_class,
            table_name  => $tname,
            operation   => 'DELETE',
            old_data    => $old,
            new_data    => undef,
        ));

        return $result;
    });
}

sub _cdc_class_delete {
    my ($self, $schema_class, $tname, @args) = @_;
    my %named = @args;
    my $where = $named{'-where'} || {};

    return $self->_cdc_ensure_atomic(sub {
        my $rows = $self->select(-where => $where);
        my @snapshots = map { _cdc_snapshot(undef, $_) } @$rows;

        my $result = $SUPER_DELETE->($self, @args);

        for my $old (@snapshots) {
            $self->_cdc_dispatch(DBIx::DataModel::Plugin::CDC::Event->build(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'DELETE',
                old_data    => $old,
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
