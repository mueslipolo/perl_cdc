package DBIx::DataModel::Plugin::CDC::Table;

use strict;
use warnings;
use parent 'DBIx::DataModel::Source::Table';

use Scalar::Util qw(blessed);
use Try::Tiny;
use DBIx::DataModel::Plugin::CDC;
use DBIx::DataModel::Plugin::CDC::Event;

# ---------------------------------------------------------------
# _cdc_table_name() -> $name | undef
#
# Returns the upper-cased table name if CDC is enabled for this
# table class, or undef if not tracked.
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
# Extracts column data from a blessed row, skipping internals.
# Keys are upper-cased for consistency.
# ---------------------------------------------------------------
sub _cdc_snapshot {
    my ($class_or_self, $obj) = @_;
    return {
        map  { uc($_) => $obj->{$_} }
        grep { !/^__/ }
        keys %$obj
    };
}

# ---------------------------------------------------------------
# _cdc_dispatch($schema, $event)
# ---------------------------------------------------------------
sub _cdc_dispatch {
    my ($self, $event) = @_;
    my $schema       = $self->schema;
    my $schema_class = $schema->metadm->class;
    DBIx::DataModel::Plugin::CDC->dispatch($schema_class, $schema, $event);
}

# ---------------------------------------------------------------
# _cdc_in_transaction() -> bool
#
# True if we're inside a do_transaction() call.
# ---------------------------------------------------------------
sub _cdc_in_transaction {
    my ($self) = @_;
    return $self->schema->{transaction_dbhs} ? 1 : 0;
}

# ---------------------------------------------------------------
# _cdc_ensure_atomic($coderef)
#
# If already in a transaction, just run the code.
# Otherwise, wrap in do_transaction for atomicity.
# ---------------------------------------------------------------
sub _cdc_ensure_atomic {
    my ($self, $code) = @_;
    my $dbh = $self->schema->dbh;

    # Already inside do_transaction or manual AutoCommit=0 — just run.
    if ($self->_cdc_in_transaction || !$dbh->{AutoCommit}) {
        return $code->();
    }

    # AutoCommit on, not in do_transaction — wrap for atomicity.
    return $self->schema->do_transaction($code);
}

# ---------------------------------------------------------------
# insert — override
# ---------------------------------------------------------------
sub insert {
    my $self = shift;
    my @args = @_;

    my $tname = $self->_cdc_table_name;
    return $self->next::method(@args) unless $tname;

    my $schema_class = $self->schema->metadm->class;
    # Resolve parent method before entering do_transaction closure
    my $super = $self->can('DBIx::DataModel::Source::Table::insert');

    return $self->_cdc_ensure_atomic(sub {
        my @results = $super->($self, @args);

        for my $rec (@args) {
            next unless ref $rec;
            my $new_data = _cdc_snapshot(undef, $rec);
            my $event = DBIx::DataModel::Plugin::CDC::Event->build(
                schema_name => $schema_class,
                table_name  => $tname,
                operation   => 'INSERT',
                old_data    => undef,
                new_data    => $new_data,
            );
            $self->_cdc_dispatch($event);
        }

        return wantarray ? @results : $results[0];
    });
}

# ---------------------------------------------------------------
# update — override
# ---------------------------------------------------------------
sub update {
    my $self = shift;
    my @args = @_;

    my $tname = $self->_cdc_table_name;
    return $self->next::method(@args) unless $tname;

    my $schema_class = $self->schema->metadm->class;
    my $super = $self->can('DBIx::DataModel::Source::Table::update');
    my $is_class_method = @args && !ref $args[0] && ($args[0] // '') =~ /^-/;

    if ($is_class_method) {
        my %named   = @args;
        my $to_set  = $named{'-set'}   || {};
        my $where   = $named{'-where'} || {};

        my $rows = $self->select(-where => $where);
        my @snapshots = map { _cdc_snapshot(undef, $_) } @$rows;

        return $self->_cdc_ensure_atomic(sub {
            my $result = $super->($self, @args);

            my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;
            for my $old (@snapshots) {
                my %new = (%$old, %changes);
                my $event = DBIx::DataModel::Plugin::CDC::Event->build(
                    schema_name => $schema_class,
                    table_name  => $tname,
                    operation   => 'UPDATE',
                    old_data    => $old,
                    new_data    => \%new,
                );
                $self->_cdc_dispatch($event);
            }

            return $result;
        });
    }

    my $old = _cdc_snapshot(undef, $self);

    return $self->_cdc_ensure_atomic(sub {
        my $result = $super->($self, @args);

        my $to_set  = ref $args[0] eq 'HASH' ? $args[0] : {};
        my %changes = map { uc($_) => $to_set->{$_} } keys %$to_set;
        my %new     = (%$old, %changes);

        my $event = DBIx::DataModel::Plugin::CDC::Event->build(
            schema_name => $schema_class,
            table_name  => $tname,
            operation   => 'UPDATE',
            old_data    => $old,
            new_data    => \%new,
        );
        $self->_cdc_dispatch($event);

        return $result;
    });
}

# ---------------------------------------------------------------
# delete — override
# ---------------------------------------------------------------
sub delete {
    my $self = shift;
    my @args = @_;

    my $tname = $self->_cdc_table_name;
    return $self->next::method(@args) unless $tname;

    my $schema_class = $self->schema->metadm->class;
    my $super = $self->can('DBIx::DataModel::Source::Table::delete');
    my $is_class_method = @args && !ref $args[0] && ($args[0] // '') =~ /^-/;

    if ($is_class_method) {
        my %named = @args;
        my $where = $named{'-where'} || {};

        my $rows = $self->select(-where => $where);
        my @snapshots = map { _cdc_snapshot(undef, $_) } @$rows;

        return $self->_cdc_ensure_atomic(sub {
            my $result = $super->($self, @args);

            for my $old (@snapshots) {
                my $event = DBIx::DataModel::Plugin::CDC::Event->build(
                    schema_name => $schema_class,
                    table_name  => $tname,
                    operation   => 'DELETE',
                    old_data    => $old,
                    new_data    => undef,
                );
                $self->_cdc_dispatch($event);
            }

            return $result;
        });
    }

    my $old = _cdc_snapshot(undef, $self);

    return $self->_cdc_ensure_atomic(sub {
        my $result = $super->($self, @args);

        my $event = DBIx::DataModel::Plugin::CDC::Event->build(
            schema_name => $schema_class,
            table_name  => $tname,
            operation   => 'DELETE',
            old_data    => $old,
            new_data    => undef,
        );
        $self->_cdc_dispatch($event);

        return $result;
    });
}

1;

__END__

=head1 NAME

DBIx::DataModel::Plugin::CDC::Table — CDC-aware table parent class

=head1 DESCRIPTION

Use as C<table_parent> when declaring a DBIx::DataModel schema.
Overrides C<insert>, C<update>, and C<delete> to capture change
events and dispatch them to registered handlers.

=cut
